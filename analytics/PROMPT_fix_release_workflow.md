# Промпт: исправить отсутствие тегов и релизов после пушей в main

## Контекст
Репозиторий: https://github.com/Reider85/dieseldb
Все пункты из `/analytics/GITHUB.md` заявлены как реализованные, но теги и GitHub Releases после пушей в `main` не появляются (на репо 0 тегов, 0 релизов). Workflow `Release on Push` не запускается вообще.

## Корневая причина (подтверждено парсером YAML)
GitHub не может загрузить `.github/workflows/release-on-push.yml` как валидный workflow. PyYAML падает с ошибкой:

```
YAML ERROR: while parsing a block collection
  in line 45, column 7
expected <block end>, but found '<block sequence start>'
  in line 117, column 8
```

Найдено 4 бага. Нужно исправить все.

## Баг 1. Сломанные отступы в `release-on-push.yml` (последние 3 шага)

В файле `.github/workflows/release-on-push.yml` три последних шага используют на 1 пробел больше, чем нужно:

| Строка | Текущий отступ | Должен быть |
|---|---|---|
| `- name: Generate javadoc link` | 7 пробелов перед `-` | **6 пробелов** |
| все дочерние поля этого шага (`id:`, `if:`, `run:`) | 9 пробелов | **8 пробелов** |
| `- name: Create GitHub Release with JAR` | 7 пробелов перед `-` | **6 пробелов** |
| все дочерние поля этого шага (`if:`, `uses:`, `with:`, `tag_name:`...) | 9 пробелов | **8 пробелов** |
| поле `body:` внутри `with:` | 12 пробелов | **10 пробелов** (вровень с `prerelease:`) |
| все строки внутри `body: \|` | 14 пробелов | **12 пробелов** |

## Баг 2. `${{ github.event.release.body }}` всегда null

В том же файле, в шаге `Create GitHub Release with JAR`, поле `body:` содержит `${{ github.event.release.body }}`. Этот контекст **null**, потому что триггер у workflow — `workflow_run`, а не `release`. Строку нужно удалить. Содержимое `body:` оставить статичным (javadoc-ссылка + призыв-star).

## Баг 3. `paths:` + `paths-ignore:` одновременно в `ci.yml`

В `.github/workflows/ci.yml` в блоке `on.push:` одновременно присутствуют `paths:` и `paths-ignore:`. Документация GitHub Actions явно запрещает такую комбинацию: «You cannot use both the paths and paths-ignore filters to filter the same event in a workflow». Нужно оставить только один фильтр.

## Баг 4. Настройка репозитория (не делается кодом)

Даже после фикса YAML теги и релизы не создадутся, если в настройках репозитория стоит дефолтный read-only Workflow permissions. Это нужно делать вручную через веб-интерфейс GitHub — отдельно зафиксировать в PROMPT_STATUS.md как инструкцию для пользователя.

---

## Что нужно сделать (пошагово)

### Шаг 1. Заменить `.github/workflows/release-on-push.yml` целиком

Содержимое (валидировано PyYAML, 9 шагов, все отступы выровнены):

```yaml
name: Release on Push

# Auto-release after every SUCCESSFUL CI run on main (GITHUB.md prompt 2).
#
# Triggered via workflow_run on CI, NOT via push: a release is created
# only after compile-gate + fast are green for that commit.
# Doc-only pushes never reach here: ci.yml skips **/*.md / docs/** / LICENSE
# / NOTICE -> no CI run -> no workflow_run event -> no release.
# pom.xml is NOT modified and nothing is pushed back to main, so this
# workflow cannot trigger CI recursively and needs no [skip ci] tag.
# Tag: v{pom-version}-build.{ci-run-number}, e.g. v3.4.18-build.762.

on:
  workflow_run:
    workflows: [CI]
    types: [completed]

# Serialize release jobs (never cancel one in-flight: a cancelled job
# means a missed release).
concurrency:
  group: release-on-push
  cancel-in-progress: false

jobs:
  release:
    name: release
    runs-on: ubuntu-24.04
    timeout-minutes: 30
    # Only a successful, push-triggered CI run of main produces a release.
    if: >-
      github.event.workflow_run.conclusion == 'success' &&
      github.event.workflow_run.event == 'push' &&
      github.event.workflow_run.head_branch == 'main'
    permissions:
      contents: write   # required to push tags and create releases
    steps:
      - name: Checkout the tested commit
        uses: actions/checkout@v4
        with:
          ref: ${{ github.event.workflow_run.head_sha }}
          fetch-depth: 0

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Compute release version
        id: version
        shell: bash
        run: |
          set -euo pipefail
          BASE=$(mvn -B help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null | tr -d '\r' | tail -n 1)
          case "$BASE" in
            [0-9]*.[0-9]*.[0-9]*) ;;
            *) echo "ERROR: cannot parse project.version from pom.xml (got: '$BASE')"; exit 1 ;;
          esac
          RUN="${{ github.event.workflow_run.run_number }}"
          if ! printf '%s' "$RUN" | grep -Eq '^[0-9]+$'; then
            echo "ERROR: workflow_run.run_number is not numeric (got: '$RUN')"
            exit 1
          fi
          VERSION="${BASE}-build.${RUN}"
          TAG="v${VERSION}"
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"
          echo "tag=$TAG" >> "$GITHUB_OUTPUT"
          echo "Release version: $TAG (base $BASE, CI run #$RUN)"

      - name: Skip if tag already exists (idempotent rerun)
        id: guard
        env:
          TAG: ${{ steps.version.outputs.tag }}
        run: |
          if git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1; then
            echo "exists=true" >> "$GITHUB_OUTPUT"
            echo "Tag $TAG already exists - skipping (rerun/duplicate)"
          else
            echo "exists=false" >> "$GITHUB_OUTPUT"
          fi

      - name: Build JAR
        if: steps.guard.outputs.exists == 'false'
        shell: bash
        run: mvn -B package -DskipTests -T 4 --no-transfer-progress 2>&1 | tee build.log

      - name: Print build failure diagnosis (last 50 lines)
        if: failure()
        run: |
          echo "==== last 50 lines of Maven log ===="
          tail -n 50 build.log || true
          echo "==== errors ===="
          grep -E 'ERROR|BUILD FAILURE' build.log | head -n 30 || true

      - name: Create and push tag
        if: steps.guard.outputs.exists == 'false'
        env:
          TAG: ${{ steps.version.outputs.tag }}
        run: |
          set -euo pipefail
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git config user.name "github-actions[bot]"
          git tag -a "$TAG" -m "Release $TAG"
          git push origin "$TAG"

      - name: Generate javadoc link
        id: javadoc-link
        if: steps.guard.outputs.exists == 'false'
        run: |
          # Strip leading "v" from the tag to get the version directory on Pages.
          VERSION="${{ steps.version.outputs.tag }}"
          VERSION="${VERSION#v}"
          JAVADOC_URL="https://reider85.github.io/dieseldb/${VERSION}/"
          echo "javadoc_url=$JAVADOC_URL" >> "$GITHUB_OUTPUT"
          echo "Javadoc URL: $JAVADOC_URL"

      - name: Create GitHub Release with JAR
        if: steps.guard.outputs.exists == 'false'
        uses: softprops/action-gh-release@v2
        with:
          tag_name: ${{ steps.version.outputs.tag }}
          generate_release_notes: true
          files: target/*.jar
          fail_on_unmatched_files: true
          draft: false
          prerelease: false
          body: |
            ## Javadoc Documentation

            API documentation for this release is available at: ${{ steps.javadoc-link.outputs.javadoc_url }}

            ---

            ⭐ **Star this repo if you find DieselDB useful** — it helps others discover it and supports the open-source community!
```

### Шаг 2. Исправить `ci.yml`

В блоке `on.push:` убрать `paths-ignore:` целиком, а исключения добавить в `paths:` через префикс `!`. Должно получиться:

```yaml
on:
  push:
    branches: [main]
    paths:
      - 'src/**'
      - 'pom.xml'
      - 'diesel/**'
      - '.github/workflows/**'
      - 'scripts/**'
      - '!**/*.md'
      - '!docs/**'
      - '!LICENSE'
      - '!NOTICE'
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 3 * * *'
  workflow_dispatch:
    inputs:
      profile:
        description: 'Profile to run (only used for manual trigger; nightly uses matrix)'
        type: choice
        options: [fast, core, concurrency, network, perf, large, all]
        required: false
        default: fast
```

(Остальную часть `ci.yml` не трогать.)

### Шаг 3. Проверить YAML локально перед коммитом

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release-on-push.yml')); yaml.safe_load(open('.github/workflows/ci.yml')); print('YAML OK')"
```

Если есть `actionlint`, прогнать дополнительно:
```bash
actionlint .github/workflows/release-on-push.yml .github/workflows/ci.yml
```

### Шаг 4. Зафиксировать в PROMPT_STATUS.md ручные настройки репозитория

Добавить в `PROMPT_STATUS.md` (или в README, раздел «Required GitHub settings») блок:

```markdown
## Required manual GitHub settings (cannot be applied by code)

1. **Settings → Actions → General → Workflow permissions**
   - Select **"Read and write permissions"** (default is "Read repository contents
     and packages permissions", which silently blocks tag pushes and release
     creation even when `permissions: contents: write` is set in YAML).
   - Tick **"Allow GitHub Actions to create and approve pull requests"**.

2. **Settings → Branches**
   - If branch protection is enabled on `main` with "Restrict pushes that create
     matching branches", verify that tag refs (`refs/tags/*`) are not blocked.
     Tags are usually not subject to branch protection, but a wildcard rule like
     `*` can match them.

3. **Settings → Secrets and variables → Actions**
   - No additional secrets are required for the release workflow. It uses the
     built-in `GITHUB_TOKEN`. `CODECOV_TOKEN` and PAT are only needed for
     coverage upload and topics update via API, not for releases.
```

### Шаг 5. Коммит

Один коммит, conventional-commits style:
```
fix(ci): repair release-on-push.yml YAML and ci.yml paths filter

- release-on-push.yml: fix 7-space indentation in last 3 steps (lines 117, 131, 145)
- release-on-push.yml: remove null github.event.release.body reference (workflow_run trigger has no release context)
- ci.yml: drop paths-ignore, merge exclusions into paths via ! prefix
  (GitHub Actions forbids paths + paths-ignore in the same event block)
- PROMPT_STATUS.md: document required manual GitHub settings (Workflow permissions = read-write)
```

---

## Критерии готовности (acceptance)

1. `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release-on-push.yml'))"` проходит без ошибок
2. `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` проходит без ошибок
3. `git grep -n '^       - name:' .github/workflows/release-on-push.yml` возвращает пусто (нет строк с 7-пробельным отступом перед `- name:`)
4. `git grep -n 'github.event.release.body' .github/workflows/` возвращает пусто
5. `git grep -nE '^[[:space:]]+paths-ignore:' .github/workflows/ci.yml` возвращает пусто
6. После мержа в main и прохождения CI: на вкладке Actions workflow "Release on Push" появляется с зелёной иконкой; создаётся тег вида `v3.4.x-build.{run_number}` и соответствующий GitHub Release с прикреплённым JAR
7. В PROMPT_STATUS.md добавлен блок "Required manual GitHub settings"

## Не делать
- Не добавлять новые шаги в workflow (только фикс существующих)
- Не менять логику версионирования (`v{pom-version}-build.{run-number}` — оставить как есть)
- Не переключать триггер с `workflow_run` на `push` (это сломает гарантию «release только после зелёного CI»)
- Не добавлять `mvn versions:set` (по дизайну ownership версии остаётся за локальным `make release`)
- Не трогать другие workflow (codeql, pages, dependabot) — они не связаны с этой проблемой

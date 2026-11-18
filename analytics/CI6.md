# CI6: Unblock the Release / Tag Pipeline — round 2

> **Status**: Diagnostic + remediation plan
> **Predecessor**: `analytics/CI5.md` (workflow_dispatch escape hatch, per-job gate, release-health)
> **Scope**: `.github/workflows/{ci,release-on-push,changelog,release-health}.yml`
> **Goal**: Every push to `main` that touches source code produces a tag + GitHub Release.

---

## 0. Executive Summary — почему после CI5 релизы всё ещё не работают

В `analytics/CI5.md` было предложено 8 промптов (CI5-P1 … CI5-P8). По
истории коммитов видно, что **применены только 6 из 8**:

| Промпт | Commit | Применён? |
|---|---|---|
| CI5-P1 (починить `fast` профиль) | `68b5579`, `77f9fde`, `2368f32`, `af0e8f9` (4 попытки) | частично — быстрые тесты вроде бы зелёные, но см. ERROR 2 |
| CI5-P2 (`workflow_dispatch` в `release-on-push.yml`) | `b3e9969` | да, но **сломан CI5-P3**, см. ERROR 1 |
| CI5-P3 (per-job gate через GitHub API) | `d7f4691` | да, но **переусердствовал** — см. ERROR 1 |
| CI5-P4 (починить `changelog.yml` tag detection) | `a5f2ae5` | да |
| CI5-P5 (исправить `"" >>` bash bug в `hybrid-ci.yml`) | `01e07d3` | да, `grep '"" >>'` пуст |
| CI5-P6 (`release-health.yml`) | `41cd057` | да, но **с багом в обработке unknown** — см. ERROR 9 |
| **CI5-P7 (verify token perms)** | — | **НЕ применён** |
| **CI5-P8 (cleanup `fast` + `workflow-summary` indentation)** | — | **НЕ применён** |

### Главная ошибка CI5 — CI5-P3 «сломал» escape hatch из CI5-P2

CI5-P3 добавил шаг `Verify upstream CI jobs` с `if: github.event_name != 'workflow_dispatch'`.
Все последующие шаги (`Build JAR`, `Create and push tag`, `Generate javadoc link`,
`Create GitHub Release with JAR`) получили условие вида:

```yaml
if: >-
  steps.guard.outputs.exists == 'false' &&
  steps.upstream.outputs.compile-gate == 'success' &&
  steps.upstream.outputs.fast == 'success'
```

На ручном запуске (`workflow_dispatch`) шаг `Verify upstream CI jobs` **пропускается**,
его output'ы `compile-gate` и `fast` остаются **пустыми строками**.
`"" != "success"` → все четыре шага молча пропускаются → `Verify JAR exists` падает
с `No JAR produced`. Экстренный выход, который должен был давать CI5-P2, **не работает**.

### Цепочка срабатываний на push (как задумано)

```
push to main (затрагивает src/** или pom.xml или .github/workflows/**)
    │
    ▼
┌──────────────────────────────────────────────┐
│ ci.yml (workflow "CI")                       │
│   compile-gate  ─►  fast  ─►  ci-failure-issue│
│   (release-gate — МЁРТВЫЙ, см. ERROR 5)     │
└──────────────────────────────────────────────┘
    │ workflow_run.completed
    ▼
┌──────────────────────────────────────────────┐
│ release-on-push.yml                          │
│   if: event == 'push' && head_branch == 'main'│
│   Verify upstream CI jobs  (if != dispatch)   │
│   Compute release version                    │
│   Skip if tag already exists                  │
│   Build JAR     ← if: upstream.compile-gate && upstream.fast   ← ЛОМАЕТ manual
│   Verify JAR exists                           │
│   Create and push tag                         │
│   Generate javadoc link                       │
│   Create GitHub Release with JAR              │
└──────────────────────────────────────────────┘
    │ workflow_run.completed
    ▼
┌──────────────────────────────────────────────┐
│ changelog.yml                                │
│   if: workflow_run.conclusion == 'success'   │
│   Get release tag (gh release list)          │
│   Generate / Format / Update Changelog.md    │
│   Commit with [skip ci]                      │
└──────────────────────────────────────────────┘
```

Каждый блок доказуемо проходит по коду, кроме:
1. шага `Build JAR` (и трёх последующих) на manual dispatch — ERROR 1;
2. шага `fast` в `ci.yml`, если хоть один flaky-тест снова красный — ERROR 2;
3. любого шага, которому нужен `GITHUB_TOKEN` с `contents: write`, если
   настройка репозитория откатилась — ERROR 3 (CI5-P7 не применён).

### Признаки из живого репозитория (https://github.com/Reider85/dieseldb)

| Сигнал | Значение |
|---|---|
| `pom.xml` version | `3.4.49` |
| Теги в репо | 0 на момент предыдущей диагностики (CI5 §0) |
| Релизы в репо | 0 на момент предыдущей диагностики (CI5 §0) |
| CI5-P1 коммиты | **4 штуки** (`68b5579`, `77f9fde`, `2368f32`, `af0e8f9`) — это не «просто финальный фикс», а серия «ещё одна попытка» |
| `release-on-push.yml` шаг `Build JAR` `if:` | требует `steps.upstream.outputs.compile-gate == 'success' && steps.upstream.outputs.fast == 'success'` — пустые на manual dispatch |
| `release-on-push.yml` шаг `Verify token` | **отсутствует** (CI5-P7 не применён) |
| `ci.yml` `fast` шаг `Detect orphaned test classes` | стоит **до** `Run fast tests` (CI5-P8 не применён) |
| `ci.yml` `workflow-summary` шаг | 11-пробельная индентация у `lint-tia-mapping`, `release-gate`, `ci-failure-issue` (CI5-P8 не применён) |
| `ci.yml` `release-gate` job | `if: startsWith(github.ref, 'refs/tags/v')`, но в `on.push` нет `tags:` — мёртвый |
| `changelog.yml` | триггер только `workflow_run`, без `workflow_dispatch` |
| `release-health.yml` `HEALTHY` env | не инициализируется для `CI_CONCL == "unknown"` |

---

## 1. Identified errors (in priority order)

### ERROR 1 — BLOCKER · `release-on-push.yml` шаг `Build JAR` (и три следующих) требуют upstream-outputs на manual dispatch

* **File**: `.github/workflows/release-on-push.yml`, шаги `Build JAR` (line 109), `Create and push tag` (line 130), `Generate javadoc link` (line 142), `Create GitHub Release with JAR` (line 152).
* **Symptom**: На `workflow_dispatch` шаг `Verify upstream CI jobs` имеет `if: github.event_name != 'workflow_dispatch'` и пропускается → его output'ы `compile-gate` и `fast` пустые. Условие `steps.upstream.outputs.compile-gate == 'success'` ложно → `Build JAR` не выполняется → `Verify JAR exists` падает с `No JAR produced`. Manual-запуск (CI5-P2) **молча игнорируется**, релиз не создаётся.
* **Root cause**: CI5-P3 написал per-job gate правильно для `workflow_run`, но забыл пробросить branch для `workflow_dispatch`. На manual dispatch нет upstream CI-рана, проверять нечего — нужно разрешать сборку без этого условия.
* **Why it blocks releases**: Экстренный escape-hatch не работает. Если автоматический триггер тоже не срабатывает (например, CI красный из-за нового flake), разработчик не может опубликовать релиз вручную.

### ERROR 2 — BLOCKER (вероятный) · `fast` job всё ещё может падать на push в `main`

* **File**: `.github/workflows/ci.yml`, job `fast`, step `Run fast tests` (line 121).
* **Symptom**: 4 коммита CI5-P1 (`68b5579`, `77f9fde`, `2368f32`, `af0e8f9`) говорят о том, что `fast` падал несколько раз подряд. Если `fast` красный → `Verify upstream CI jobs` в `release-on-push.yml` падает на `exit 1` → workflow `failure` → нет тега, нет релиза.
* **Verify**: нужно скачать `reports-fast` артефакт с последнего упавшего CI-рана и проверить, какие тесты красные.
* **Why it blocks releases**: `release-on-push.yml` прямо требует `fast == success`.

### ERROR 3 — HIGH · CI5-P7 (verify token perms) не применён

* **File**: `.github/workflows/release-on-push.yml`.
* **Symptom**: В шаге `Create and push tag` стоит `git push origin "$TAG"`. Если в настройках репозитория `Settings → Actions → General → Workflow permissions` стоит `Read repository contents permission` (а не `Read and write permissions`), `git push` падает с `fatal: could not read Username for 'https://github.com'` или 403. Сообщение об ошибке закопано на 1000 строк лога в `git push`, а не в начале workflow.
* **Fix**: применить CI5-P7 — добавить шаг `Verify token can push tags` сразу после `actions/checkout`.

### ERROR 4 — HIGH · CI5-P8 (cleanup `fast` + `workflow-summary` indentation) не применён

* **File**: `.github/workflows/ci.yml`.
* **Symptom 1**: В job `fast` шаг `Detect orphaned test classes` стоит **до** `Run fast tests` (line 106 vs 121). На свежем runner'е `target/test-classes` пуст → `cls_count=0` → шаг проходит вакуумно. На stale cache он может провалиться по .class-файлам от предыдущего коммита — false negative для текущего кода.
* **Symptom 2**: В `workflow-summary` шаге (line 822+) три строки имеют 11-пробельную индентацию вместо 10:
  * `           echo "| lint-tia-mapping | ..."` (line 843)
  * `           echo "| release-gate | ..."` (line 844)
  * `           echo "| ci-failure-issue | ..."` (line 845)
  * Все остальные `echo "| ... |"` строки в этом же `run:` блоке — 10 пробелов.
  * Это не синтаксическая ошибка (literal block scalar съедает лишний пробел), но это drift, который мешает читать код и копировать diff'ы.
* **Fix**: применить CI5-P8.

### ERROR 5 — HIGH · `release-gate` job в `ci.yml` — мёртвый код

* **File**: `.github/workflows/ci.yml`, job `release-gate` (line 712).
* **Code**:
  ```yaml
  release-gate:
    name: all-tests
    if: startsWith(github.ref, 'refs/tags/v')
  ```
* **Bug**: `ci.yml` триггерится `on.push.branches: [ main ]` и `on.pull_request.branches: [ main ]`, **без** `on.push.tags:`. Значит `github.ref` всегда вида `refs/heads/main`, никогда `refs/tags/v...`. Job **никогда не выполняется**. Он упомянут в `workflow-summary.needs: [..., release-gate, ...]` и помечается `skipped`, что само по себе безобидно, но:
  * misleading: создаёт впечатление, что есть ещё одна проверка перед релизом;
  * расходует ~30 строк YAML-кода, который никогда не выполнится;
  * если кто-то добавит `on.push.tags: ['v*']`, чтобы «оживить» его, он начнёт запускать тяжёлый `mvn verify -P all` на каждый тег — а теги теперь пушит `release-on-push.yml`, получим **рекурсивный запуск**: push tag → ci.yml → release-gate → `mvn verify` (30+ минут). Лучше удалить совсем.
* **Fix**: удалить job целиком; убрать его из `workflow-summary.needs`.

### ERROR 6 — HIGH · `changelog.yml` не имеет `workflow_dispatch` триггера

* **File**: `.github/workflows/changelog.yml`, line 13.
* **Code**:
  ```yaml
  on:
    workflow_run:
      workflows: [Release on Push]
      types: [completed]
  ```
* **Symptom**: После manual-релиза (CI6-P1) `changelog.yml` не запустится — `workflow_run` событие от `release-on-push.yml` будет, но если вы вручную выкатили релиз через `workflow_dispatch` в `release-on-push.yml`,下游 чейн changelog'а зависит от того, что `release-on-push.yml` завершится `success`. Это работает, **но**:
  * Нельзя вручную перегенерировать Changelog.md, если автогенерация сломалась;
  * Нельзя вручную запустить changelog для произвольного tag'а.
* **Fix**: добавить `workflow_dispatch:` и ослабить job-level `if:`.

### ERROR 7 — MEDIUM · `Compute release version` всё ещё использует медленный `mvn help:evaluate`

* **File**: `.github/workflows/release-on-push.yml`, step `Compute release version` (line 79).
* **Code**:
  ```bash
  BASE=$(mvn -B help:evaluate -Dexpression=project.version -q -DforceStdout | tr -d '\r' | awk 'NF{p=$0} END{print p}')
  ```
* **Bug**: `mvn help:evaluate` на холодном кэше — 15-20 секунд; `awk 'NF{p=$0} END{print p}'` берёт последнюю непустую строку — если maven-help-plugin напечатает warning в stdout (а не stderr), он станет «версией». CI5-P10 (LOW) предлагал python-парсер; промпт не применён.
* **Fix**: парсить `pom.xml` напрямую через python.

### ERROR 8 — MEDIUM · `Verify upstream CI jobs` молча считает отсутствующий job неуспешным

* **File**: `.github/workflows/release-on-push.yml`, step `Verify upstream CI jobs` (line 35).
* **Code**:
  ```bash
  conclusion=$(gh api repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/jobs \
    --jq ".jobs[] | select(.name == \"$job\") | .conclusion")
  echo "$job=$conclusion" >> "$GITHUB_OUTPUT"
  [ "$conclusion" = "success" ] || { echo "::error::Upstream job $job is $conclusion — refusing to release"; exit 1; }
  ```
* **Bug**: Если job `compile-gate` или `fast` будет переименован, `gh api ... --jq` вернёт пустую строку. Текст `::error::Upstream job $job is  — refusing to release` с двойным пробелом не объясняет причину. Шаг молча падает, лог выглядит как «job не success», хотя на самом деле «job не найден».
* **Fix**: добавить явную проверку `if [ -z "$conclusion" ]; then echo "::error::Job $job not found in CI run $RUN_ID — likely renamed"; exit 1; fi`.

### ERROR 9 — MEDIUM · `release-health.yml` не обрабатывает unknown CI conclusion

* **File**: `.github/workflows/release-health.yml`, step `Diagnose release pipeline` (line 22).
* **Bug**: В блоке `if [ "$CI_CONCL" != "success" ]; then ... elif ...; else ...; fi`:
  * `CI_CONCL = "failure"` → `HEALTHY=1` ✅
  * `CI_CONCL = "success"` + `LATEST_TAG = "none"` → `HEALTHY=1` ✅
  * `CI_CONCL = "success"` + `LATEST_TAG = "v3.4.49-build.820"` → `HEALTHY=0` ✅
  * **`CI_CONCL = "unknown"`** (например, `gh run list` упал, или `jq` вернул `null`, или run был `cancelled`) → `CI_CONCL != "success"` истинно → `HEALTHY=1` ✅ (это корректно — unknown тоже блокирует релизы).
  * **НО**: `LATEST_TAG` может прийти непустым даже когда `CI_CONCL = "unknown"` (если релиз был раньше, но CI красный сейчас). Тогда ветка `if [ "$CI_CONCL" != "success" ]` сработает, `HEALTHY=1`. ✅
  * Проверил по логике — на самом деле баг не критичный. **Но**: если `gh run list` ничего не вернёт (rate limit / нет runs), `LATEST_CI` = `null` (или пустой массив), `.conclusion` = `null` → `jq -r '.conclusion // "unknown"'` = `"unknown"` → `CI_CONCL != "success"` → `HEALTHY=1` ✅. На самом деле эта ветка работает.
* **Реальная проблема**: если `gh run list --workflow=release-on-push.yml` упадёт (rate limit), `REL_CONCL` будет `unknown`, но это не повлияет на HEALTHY — проверка только по `CI_CONCL` и `LATEST_TAG`. Логика OK.
* **Мелкий недочёт**: нет обработки случая, когда `HEALTHY` не задан (например, if/elif/else не покрыл какую-то комбинацию). Текущее покрытие полное, но `set -uo pipefail` + обращение к `$HEALTHY` без `${HEALTHY:-1}` — это потенциальный `unbound variable` exit. Нужно `HEALTHY="${HEALTHY:-1}"` перед `echo "healthy=$HEALTHY"`.
* **Fix**: инициализировать `HEALTHY=1` перед блоком if.

### ERROR 10 — LOW · `Compute release version` `BASE` filter принимает `3.4.49-SNAPSHOT`

* **File**: `.github/workflows/release-on-push.yml`, step `Compute release version` (line 79).
* **Code**:
  ```bash
  case "$BASE" in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *) echo "ERROR: bad project.version '$BASE'"; exit 1 ;;
  esac
  ```
* **Bug**: Шаблон `[0-9]*.[0-9]*.[0-9]*` матчит `3.4.49-SNAPSHOT` (т.к. `*` после третьей цифры разрешает любой суффикс). Если в `pom.xml` останется `-SNAPSHOT`, тег будет `v3.4.49-SNAPSHOT-build.820` — GitHub не опубликует prerelease-семантику корректно, и в Changelog это попадёт как версия.
* **Fix**: либо `case "$BASE" in [0-9]*.[0-9]*.[0-9]*-SNAPSHOT) BASE="${BASE%-SNAPSHOT}";; [0-9]*.[0-9]*.[0-9]*) ;; *) ... exit 1 ;; esac`, либо вычислять `BASE` через `mvn help:evaluate -Dexpression=project.version -q -DforceStdout` и сразу `sed 's/-SNAPSHOT$//'`.

---

## 2. Prompts to fix each error

Каждый промпт ниже самодостаточен и может быть передан LLM или
разработчику по порядку. Промпты 1-3 — блокирующие; 4-7 — hardening;
8-10 — cleanup.

---

### Prompt CI6-P1 — Починить escape-hatch: `Build JAR` и последующие шаги должны работать на `workflow_dispatch`

**Goal**: Сделать так, чтобы `gh workflow run release-on-push.yml -f commit_sha=<sha>` действительно публиковал тег и релиз.

**Diagnosis**: Шаг `Verify upstream CI jobs` имеет `if: github.event_name != 'workflow_dispatch'` и пропускается на manual-запуске, оставляя output'ы `compile-gate` и `fast` пустыми. Все последующие шаги проверяют `steps.upstream.outputs.compile-gate == 'success' && steps.upstream.outputs.fast == 'success'` и пропускаются.

**Patch** `.github/workflows/release-on-push.yml`:

1. В шаге `Verify upstream CI jobs` (line ~37) установить output'ы по умолчанию для manual-режима, **чтобы** downstream-шаги видели непустые значения. Лучше: отделить gate-логику от output-логики. А именно — переписать шаг:

```yaml
      - name: Verify upstream CI jobs
        id: upstream
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          RUN_ID: ${{ github.event.workflow_run.id }}
          EVENT_NAME: ${{ github.event_name }}
        run: |
          set -euo pipefail
          # На workflow_dispatch upstream CI run отсутствует — выходные success,
          # чтобы downstream-шаги (Build JAR, Create tag, ...) выполнились.
          # Ответственность за выбор коммита лежит на операторе, запустившем workflow.
          if [ "$EVENT_NAME" = "workflow_dispatch" ]; then
            echo "compile-gate=success" >> "$GITHUB_OUTPUT"
            echo "fast=success" >> "$GITHUB_OUTPUT"
            echo "Manual dispatch — upstream gate bypassed (operator is responsible for the commit)."
            exit 0
          fi
          for job in compile-gate fast; do
            conclusion=$(gh api "repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/jobs" \
              --jq ".jobs[] | select(.name == \"$job\") | .conclusion")
            if [ -z "$conclusion" ]; then
              echo "::error::Job '$job' not found in CI run $RUN_ID — likely renamed."
              exit 1
            fi
            echo "$job=$conclusion" >> "$GITHUB_OUTPUT"
            echo "Upstream job $job: $conclusion"
            [ "$conclusion" = "success" ] || { echo "::error::Upstream job $job is $conclusion — refusing to release"; exit 1; }
          done
```

2. Условия `if:` в шагах `Build JAR`, `Create and push tag`, `Generate javadoc link`, `Create GitHub Release with JAR` **оставить как есть** — теперь они пройдут, потому что на manual dispatch output'ы будут `success` (см. шаг 1). На `workflow_run` всё остаётся как было.

3. Дополнительная защита: шаг `Verify JAR exists` (line ~118) должен работать и на manual dispatch. Его `if:` уже `steps.guard.outputs.exists == 'false'` (без upstream-check'а), так что всё OK.

**Acceptance criteria**
* `gh workflow run release-on-push.yml --repo Reider85/dieseldb -f commit_sha=<sha>` завершается `success` и создаёт тег `v3.4.49-build.<run_number>` (или `v3.4.49-<version_override>` если задан override).
* `gh workflow run release-on-push.yml -f dry_run=true` печатает имя тега и **не** пушит его (проверить через `gh release list`).
* На обычном `workflow_run` (CI зелёный) поведение не меняется.

---

### Prompt CI6-P2 — Проверить, что `fast` профиль действительно зелёный

**Goal**: Подтвердить, что CI5-P1 (4 коммита) действительно починил `fast`, и если нет — додиагностировать.

**Steps**

1. Получить последний CI-ран на `main`:
   ```bash
   gh run list --repo Reider85/dieseldb --workflow=ci.yml --branch=main --limit=5 \
     --json databaseId,conclusion,headSha,createdAt --jq '.[] | "\(.databaseId)\t\(.conclusion)\t\(.headSha[:8])\t\(.createdAt)"'
   ```
2. Если последний ран `failure` — скачать артефакт `reports-fast`:
   ```bash
   RUN_ID=<id из шага 1>
   gh run download "$RUN_ID" --repo Reider85/dieseldb --name reports-fast -D /tmp/reports-fast
   ```
3. Найти упавшие тесты:
   ```bash
   grep -rlE '<failure|<error' /tmp/reports-fast/ | head -20
   grep -hE 'testsuite name=|<failure |<error ' /tmp/reports-fast/*.xml | head -40
   ```
4. Если падежа нет (`grep` пуст) — значит `fast` зелёный, проблема не в нём. Переходите к CI6-P1.
5. Если падёж есть — для каждого `<failure>` / `<error>`:
   * Реальный регресс → чинить production-код или тест.
   * Порядок/тайминги → `@Tag("flaky")` + добавить в `flaky` profile в `pom.xml`, **не** оставлять в `smoke`/`query`/`index`.
   * Окружение (port/temp/locale) → чинить setup, **не** ослаблять assertion.
6. Воспроизвести локально:
   ```bash
   mvn -B -T 4 -Dstyle.color=never test -P fast
   ```
   Acceptance: exit code 0, `target/surefire-reports/` заполнен, ни одного `<failure>` / `<error>`.
7. Коммит: `CI6-P2: fix failing fast-profile test (<ClassName>)`, push в `main`.

**Acceptance criteria**
* Следующий CI-ран на `main` имеет `fast.result == success`.
* `release-on-push.yml` для этого рана **не** skipped и **не** failed в шаге `Verify upstream CI jobs`.

---

### Prompt CI6-P3 — Применить CI5-P7: verify-token-permission шаг

**Goal**: Быстро падать с понятной ошибкой, если репо-настройка `contents: write` откатилась.

**Patch** `.github/workflows/release-on-push.yml`: добавить шаг сразу после `actions/checkout` (перед `Verify upstream CI jobs`):

```yaml
      - name: Verify token can push tags
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -euo pipefail
          PERM_JSON=$(gh api "repos/$GITHUB_REPOSITORY" --jq '.permissions')
          echo "Repository permissions: $PERM_JSON"
          CAN_PUSH=$(echo "$PERM_JSON" | jq -r '.push')
          if [ "$CAN_PUSH" != "true" ]; then
            echo "::error::GITHUB_TOKEN lacks push permission. Enable Settings → Actions → General → Workflow permissions → 'Read and write permissions'."
            exit 1
          fi
          echo "Token can push — tag push will succeed."
```

**Acceptance criteria**
* Если настройка репо откатывается в `Read`, workflow падает на этом шаге за ~5 секунд с понятным `::error::`, а не через 10 минут в `git push origin`.
* На нормальной настройке шаг печатает `Token can push — tag push will succeed.`.

---

### Prompt CI6-P4 — Применить CI5-P8: cleanup `fast` job + `workflow-summary` индентация

**Goal**: Убрать вакуумные/фейковые шаги и нормализовать индентацию.

**Patch 1** `.github/workflows/ci.yml`, job `fast` (line ~106): удалить шаг `Detect orphaned test classes` целиком. Эта проверка принадлежит `compile-gate` (там она уже есть на line 222 — пусть остаётся), а в `fast` она работает до `mvn test` и бесполезна на свежем runner'е.

**Patch 2** `.github/workflows/ci.yml`, job `workflow-summary` (lines 843-845): сменить 11-пробельную индентацию на 10-пробельную. Конкретно — найти строки

```
           echo "| lint-tia-mapping | ${{ needs.lint-tia-mapping.result }} |" >> $GITHUB_STEP_SUMMARY
           echo "| release-gate | ${{ needs.release-gate.result }} |" >> $GITHUB_STEP_SUMMARY
           echo "| ci-failure-issue | ${{ needs.ci-failure-issue.result }} |" >> $GITHUB_STEP_SUMMARY
```

и заменить ведущие 11 пробелов на 10:

```
          echo "| lint-tia-mapping | ${{ needs.lint-tia-mapping.result }} |" >> $GITHUB_STEP_SUMMARY
          echo "| release-gate | ${{ needs.release-gate.result }} |" >> $GITHUB_STEP_SUMMARY
          echo "| ci-failure-issue | ${{ needs.ci-failure-issue.result }} |" >> $GITHUB_STEP_SUMMARY
```

**Verify**:
```bash
grep -n 'Detect orphaned test classes' .github/workflows/ci.yml
# ожидается: matches в compile-gate, pr-gate-tia, pr-gate, nightly, nightly-flaky, profile-stability, manual — но НЕ в fast
awk '/^  workflow-summary:/{f=1} f && /^  [a-z]/{exit} f && /echo "\|/ {n=match($0,/[^ ]/); print NR, n-1}' .github/workflows/ci.yml
# ожидается: все строки показывают одно значение (10)
```

**Acceptance criteria**
* `grep -n 'Detect orphaned test classes' .github/workflows/ci.yml` НЕ содержит совпадений в `fast` job (между `# ──────── Push gate` и `# ──────── TIA`).
* Все `echo "| ... |"` строки в `workflow-summary` имеют одинаковую индентацию (10 пробелов).

---

### Prompt CI6-P5 — Удалить мёртвый `release-gate` job из `ci.yml`

**Goal**: Убрать 50+ строк мёртвого кода, который вводит в заблуждение.

**Patch** `.github/workflows/ci.yml`:

1. Удалить job `release-gate` целиком (lines 712-762).
2. В job `workflow-summary` (line 826) убрать `release-gate` из массива `needs:`:

```yaml
  workflow-summary:
    name: Workflow Summary
    runs-on: ubuntu-24.04
    if: always()
    needs: [compile-gate, fast, pr-gate-tia, pr-gate, nightly, nightly-flaky, profile-stability, manual, ci-failure-issue, lint-tia-mapping]
```

3. Удалить строку `echo "| release-gate | ..."` из `workflow-summary` шага `Generate summary`.

**Why safe**: Этот job никогда не запускался (нет `on.push.tags:` в `ci.yml`). Если нужна проверка «all tests pass» перед релизом — её делает `release-on-push.yml::Verify upstream CI jobs`, который требует `compile-gate` и `fast` success. Полный `mvn verify -P all` и так跑ается в nightly.

**Acceptance criteria**
* `grep -n 'release-gate:' .github/workflows/ci.yml` возвращает 0 совпадений.
* `grep -n 'release-gate' .github/workflows/ci.yml` возвращает 0 совпадений.
* `workflow-summary` шаг не падает на undefined `${{ needs.release-gate.result }}`.

---

### Prompt CI6-P6 — Добавить `workflow_dispatch` в `changelog.yml`

**Goal**: Разрешить ручную перегенерацию Changelog.md после manual-релиза.

**Patch** `.github/workflows/changelog.yml`:

1. Заменить блок `on:` (lines 13-15):

```yaml
on:
  workflow_run:
    workflows: [Release on Push]
    types: [completed]
  workflow_dispatch:
    inputs:
      tag_name:
        description: 'Tag to generate changelog for (e.g. v3.4.49-build.820). Empty = latest release.'
        required: false
```

2. Заменить job-level `if:` (line 24):

```yaml
    if: >-
      github.event_name == 'workflow_dispatch' ||
      github.event.workflow_run.conclusion == 'success'
```

3. В шаге `Get release tag` (line 27) учесть ручной ввод:

```yaml
      - name: Get release tag
        id: get-tag
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          INPUT_TAG: ${{ github.event.inputs.tag_name }}
        run: |
          set -euo pipefail
          if [ -n "$INPUT_TAG" ]; then
            TAG="$INPUT_TAG"
          else
            TAG=$(gh release list --repo "$GITHUB_REPOSITORY" --limit 1 --json tagName --jq '.[0].tagName // empty')
          fi
          if [ -z "$TAG" ]; then
            echo "No published release found — nothing to changelog."
            exit 0
          fi
          VERSION="${TAG#v}"
          BASE_VERSION="${VERSION%-build.*}"
          BASE_VERSION="${BASE_VERSION%-manual.*}"
          BASE_VERSION="${BASE_VERSION%-*}"
          echo "tag=$TAG"        >> "$GITHUB_OUTPUT"
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"
          echo "base_version=$BASE_VERSION" >> "$GITHUB_OUTPUT"
          echo "Release tag: $TAG (base $BASE_VERSION)"
```

**Acceptance criteria**
* `gh workflow run changelog.yml -f tag_name=v3.4.49-build.820` обновляет `Changelog.md` для указанного тега.
* Обычный автозапуск после `release-on-push.yml` работает как прежде.

---

### Prompt CI6-P7 — Заменить `mvn help:evaluate` на python-парсер `pom.xml`

**Goal**: Убрать 15-20 секунд холодного старта и хрупкость `awk`.

**Patch** `.github/workflows/release-on-push.yml`, шаг `Compute release version` (line 79):

```yaml
      - name: Compute release version
        id: version
        shell: bash
        run: |
          set -euo pipefail
          BASE=$(python3 -c '
          import re, xml.etree.ElementTree as ET
          root = ET.parse("pom.xml").getroot()
          ns = ""
          if root.tag.startswith("{"):
              ns = root.tag.split("}")[0] + "}"
          v = root.find(f"{ns}version")
          if v is None:
              v = root.find(f"{ns}parent/{ns}version")
          if v is None or v.text is None:
              raise SystemExit("ERROR: cannot find <version> in pom.xml")
          print(v.text.strip())
          ')
          # Strip -SNAPSHOT if present
          BASE="${BASE%-SNAPSHOT}"
          case "$BASE" in
            [0-9]*.[0-9]*.[0-9]*) ;;
            *) echo "ERROR: bad project.version '$BASE'"; exit 1 ;;
          esac
          if [ -n "${{ github.event.inputs.version_override }}" ]; then
            VERSION="${BASE}-${{ github.event.inputs.version_override }}"
          else
            RUN="${{ github.event.workflow_run.run_number || github.run_number }}"
            printf '%s' "$RUN" | grep -Eq '^[0-9]+$' || { echo "ERROR: run_number not numeric: '$RUN'"; exit 1; }
            VERSION="${BASE}-build.${RUN}"
          fi
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"
          echo "tag=v${VERSION}" >> "$GITHUB_OUTPUT"
          echo "Release version: v${VERSION} (base $BASE)"
```

**Acceptance criteria**
* `Compute release version` шаг занимает <1 сек (а не 15-20).
* Если `pom.xml` содержит `<version>3.4.49-SNAPSHOT</version>`, тег будет `v3.4.49-build.N`, а не `v3.4.49-SNAPSHOT-build.N`.
* Если `pom.xml` не содержит `<version>` ни в корне, ни в `<parent>` — шаг падает с `ERROR: cannot find <version> in pom.xml`, а не с непонятным `bad project.version ''`.

---

### Prompt CI6-P8 — Добавить guard «job not found» в `Verify upstream CI jobs`

**Goal**: Не молчать, если job был переименован.

**Patch** `.github/workflows/release-on-push.yml`, шаг `Verify upstream CI jobs` (внутри цикла `for job in compile-gate fast; do`):

```bash
          for job in compile-gate fast; do
            conclusion=$(gh api "repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/jobs" \
              --jq ".jobs[] | select(.name == \"$job\") | .conclusion")
            if [ -z "$conclusion" ]; then
              echo "::error::Job '$job' not found in CI run $RUN_ID — likely renamed. Update release-on-push.yml gate list."
              exit 1
            fi
            echo "$job=$conclusion" >> "$GITHUB_OUTPUT"
            echo "Upstream job $job: $conclusion"
            [ "$conclusion" = "success" ] || { echo "::error::Upstream job $job is $conclusion — refusing to release"; exit 1; }
          done
```

(Это уже включено в патч из CI6-P1, шаг 1. Если CI6-P1 применён отдельно, продублируйте эту проверку здесь.)

**Acceptance criteria**
* Если в `ci.yml` job `compile-gate` будет переименован в `compile`, `release-on-push.yml` упадёт с `::error::Job 'compile-gate' not found in CI run <id> — likely renamed.`.

---

### Prompt CI6-P9 — Инициализировать `HEALTHY=1` в `release-health.yml`

**Goal**: Не уронить workflow на `set -uo pipefail` в нестандартных ситуациях.

**Patch** `.github/workflows/release-health.yml`, шаг `Diagnose release pipeline`:

Перед блоком `if [ "$CI_CONCL" != "success" ]; then ... fi` добавить:

```bash
          HEALTHY=1   # default to unhealthy; only set to 0 when explicitly healthy
```

Это гарантирует, что переменная всегда определена, и `echo "healthy=$HEALTHY" >> "$GITHUB_ENV"` не упадёт на `unbound variable` в каких-то непредусмотренных ветках.

**Acceptance criteria**
* `release-health.yml` не падает на `set -u`, если ни одна из веток `if/elif/else` не сработала.

---

### Prompt CI6-P10 — Удалить `-SNAPSHOT` из `BASE` в `Compute release version`

(Уже включено в патч CI6-P7 как `BASE="${BASE%-SNAPSHOT}"`. Если CI6-P7 не применяется, вынести в отдельный мини-промпт.)

**Goal**: Не публиковать тег `v3.4.49-SNAPSHOT-build.N`.

**Patch** `.github/workflows/release-on-push.yml`, шаг `Compute release version`, после строки `BASE=$(...)`:

```bash
          BASE="${BASE%-SNAPSHOT}"
```

**Acceptance criteria**
* `pom.xml` с `<version>3.4.49-SNAPSHOT</version>` производит тег `v3.4.49-build.N`, а не `v3.4.49-SNAPSHOT-build.N`.

---

## 3. Suggested application order

| # | Prompt | Unblocks releases? | Risk | Effort |
|---|---|---|---|---|
| 1 | **CI6-P1** (escape-hatch fix) | ✅ directly (manual dispatch работает) | low | small |
| 2 | **CI6-P3** (verify token perms) | early failure signal | low | small |
| 3 | **CI6-P2** (verify `fast` is green) | ✅ напрямую (если CI красный — ничего не выйдет) | medium (needs artifact analysis) | medium |
| 4 | CI6-P8 (job-not-found guard) | robustness | low | trivial |
| 5 | CI6-P5 (remove dead `release-gate`) | cleanup | low | small |
| 6 | CI6-P4 (cleanup `fast` + indentation) | robustness | low | small |
| 7 | CI6-P6 (`workflow_dispatch` для changelog) | manual changelog | low | small |
| 8 | CI6-P7 (python parser для version) | speed + correctness | low | small |
| 9 | CI6-P9 (HEALTHY init) | robustness | low | trivial |
| 10 | CI6-P10 (strip -SNAPSHOT) | correctness | low | trivial |

**Минимальный набор, чтобы следующий push в `main` опубликовал релиз**:
1. CI6-P1 (manual escape hatch) — на случай если автотриггер всё ещё сломан;
2. CI6-P3 (verify token perms) — чтобы сразу видеть проблему с настройками;
3. CI6-P2 (verify `fast` is green) — собственно условие срабатывания.

Если CI5-P1 действительно починил `fast`, то после применения CI6-P1 + CI6-P3
следующий push в `main`, затрагивающий `src/**`, **должен** опубликовать тег и
релиз. Если нет — CI6-P2 скажет, какой тест всё ещё красный.

---

## 4. Verification checklist

После применения CI6-P1 + CI6-P3 + CI6-P2:

- [ ] `gh workflow run release-on-push.yml --repo Reider85/dieseldb -f commit_sha=<known-good-sha>` завершается `success`.
- [ ] В `https://github.com/Reider85/dieseldb/tags` появляется тег `v3.4.49-build.<N>` (или `v3.4.49-<version_override>`).
- [ ] В `https://github.com/Reider85/dieseldb/releases` появляется GitHub Release `v3.4.49-build.<N>` с прикреплённым JAR.
- [ ] `gh workflow run release-on-push.yml -f dry_run=true` печатает имя тега и **не** пушит его.
- [ ] Если в `Settings → Actions → General → Workflow permissions` стоит `Read`, `release-on-push.yml` падает на шаге `Verify token can push tags` за ~5 секунд с `::error::GITHUB_TOKEN lacks push permission...`.
- [ ] Последний CI-ран на `main` имеет `compile-gate=success` AND `fast=success` (если нет — CI6-P2 не завершён).
- [ ] `release-on-push.yml` для этого CI-рана **не** skipped и **не** failed.

После применения CI6-P4 + CI6-P5 + CI6-P6 + CI6-P7 + CI6-P8 + CI6-P9 + CI6-P10:

- [ ] `grep -n 'Detect orphaned test classes' .github/workflows/ci.yml` НЕ содержит совпадений в `fast` job.
- [ ] `awk` проверка `workflow-summary` показывает одинаковую индентацию для всех `echo "| ... |"` строк.
- [ ] `grep -n 'release-gate:' .github/workflows/ci.yml` пусто.
- [ ] `changelog.yml` можно запустить через `gh workflow run changelog.yml -f tag_name=v...`.
- [ ] Шаг `Compute release version` выполняется <1 сек.
- [ ] Если `pom.xml` содержит `-SNAPSHOT`, тег не содержит `-SNAPSHOT`.
- [ ] Если job `compile-gate` или `fast` переименован в `ci.yml`, `release-on-push.yml` падает с `::error::Job '<name>' not found in CI run <id>`.
- [ ] `release-health.yml` не падает на `set -u`.

---

## 5. What CI5 got right and what CI6 adds

| | CI5 (P1–P8) | CI6 (P1–P10) |
|---|---|---|
| Fixed `fast` profile (4 коммита) | ✅ (предположительно) | CI6-P2 — verify |
| Added `workflow_dispatch` to `release-on-push.yml` | ✅ CI5-P2 | — |
| Replaced workflow-level conclusion with per-job gate | ✅ CI5-P3 | CI6-P1 — **исправляет баг**, который CI5-P3 внёс в manual-режим |
| Fixed `changelog.yml` `head_ref` bug | ✅ CI5-P4 | — |
| Fixed `hybrid-ci.yml` `"" >>` bash bug | ✅ CI5-P5 | — |
| Added `release-health.yml` observability | ✅ CI5-P6 | CI6-P9 — `HEALTHY` initialization |
| **Verified token can push tags** | ❌ CI5-P7 не применён | ✅ CI6-P3 |
| **Cleaned up `fast` + `workflow-summary` indentation** | ❌ CI5-P8 не применён | ✅ CI6-P4 |
| **Fixed escape-hatch broken by CI5-P3** | n/a | ✅ CI6-P1 |
| **Removed dead `release-gate` job** | n/a | ✅ CI6-P5 |
| **Added `workflow_dispatch` to `changelog.yml`** | n/a | ✅ CI6-P6 |
| **Replaced `mvn help:evaluate` with python parser** | ❌ CI5-P10 LOW не применён | ✅ CI6-P7 |
| **Added job-not-found guard** | n/a | ✅ CI6-P8 |
| **Strip `-SNAPSHOT` from version** | n/a | ✅ CI6-P10 |

### Краткая диагностика в одном предложении

CI5 починил «способность» пайплайна пушить теги и добавил escape-hatch, но
CI5-P3 случайно сломал этот escape-hatch переусердствовшим `if:` на upstream
output'ах, а CI5-P7 и CI5-P8 так и не были применены — поэтому даже после
шести коммитов CI5 ручной запуск `release-on-push.yml` падает с
`No JAR produced`, а автоматический может не сработать, если `fast` снова
красный или если в репо откатилась настройка `contents: write`.

---

*End of CI6.*

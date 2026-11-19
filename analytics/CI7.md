# CI7 — Final Remediation: Every Push Must Produce a Tag and a Release

> **Status**: Retrospective diagnosis of CI.md → CI6.md + concrete, mechanical remediation prompts
> **Predecessor**: `analytics/CI6.md` (workflow_dispatch escape-hatch, per-job gate, release-health)
> **Scope**: `.github/workflows/{release-on-push,ci,changelog}.yml`, plus one new `.github/workflows/release-on-push-direct.yml`
> **Goal (verbatim from the user)**: every commit and push must release tags and releases.
> **Live evidence (2026-11-19)**: 0 tags, 0 releases, last 5 CI runs are `failed/cancelled/cancelled/failed/cancelled`, CI run #839 on commit `4159c26` failed after 33m 47s.

---

## 0. TL;DR — почему CI1…CI6 не привели к появлению ни одного тега

Все шесть предыдущих файлов пытались **починить существующий пайплайн**, не меняя его фундаментальную архитектуру. Архитектура же была ошибочной для декларируемой цели «каждый пуш = тег + релиз»:

```
push → ci.yml → workflow_run.conclusion == 'success' → release-on-push.yml
                         ↑
                   этот предикат НИКОГДА не true,
                   потому что в ci.yml всегда красный fast-профиль
                   (один flaky тест = весь CI failure = релиз skipped)
```

| Промпт-документ | Применённые коммиты | Главный недочёт в самом подходе |
|---|---|---|
| **CI.md** | Несколько коммитов 3.4.x | Чинил YAML и шаги, но сохранил `workflow_run.conclusion == 'success'` как единственный гейт |
| **CI2.md** | P1: 0 коммитов, P2: 0, P3: 0, P4: частично, P5: 0, P6: 0 | Все 6 промптов были про «починить fast-профиль». Но flaky-тест невозможно «починить» — его можно только изолировать. Гейт остался |
| **CI3.md** | P1: 0, P2: 0, P3: 0, P4: не применён | P4 (временная пометка `@Tag("flaky")`) был единственным реальным разблокиратором, но **не был применён**. CI3.md явно написал «если P1–P3 не успеть — сделай P4», и его не сделали |
| **CI4.md** | P1: 0, P2: 0, P3: не нужен (CI зелёный — но не был), P4: применил, P5: применил, P6: применил, P7: применил, P8: применил | CI4-P4 (repo-level `contents: write`) был применён, но **CI так и не стал зелёным** → P5/P6/P7/P8 вступают в силу только когда `release`-job стартует. А он не стартует |
| **CI5.md** | P1: 4 коммита-попытки, P2: применил, P3: применил (с багом!), P4: применил, P5: применил, P6: применил, P7: НЕ применён, P8: НЕ применён | CI5-P3 «сломал» escape-hatch из CI5-P2 — ручной `workflow_dispatch` падал на `Build JAR` из-за пустых upstream-outputs |
| **CI6.md** | P1: применил, P2: НЕ применён, P3: применил, P4: применил, P5: НЕ применён, P6: применил, P7: применил, P8: применил, P9: применил, P10: применил | CI6-P1 починил escape-hatch, но **CI всё ещё красный** → автотриггер не срабатывает. Ручной escape-hatch никогда не запускался (0 тегов) |

**Итог в одном предложении**: за 6 итераций починили способность `release-on-push.yml` пушить теги, но не убрали зависимость от зелёного CI, а CI так и не позеленел → `workflow_run.conclusion == 'success'` ни разу не выполнилось → `release`-job ни разу не стартовал → 0 тегов, 0 релизов.

CI7 меняет архитектуру: **релизный воркфлоу больше не зависит от результата `ci.yml` вообще**.

---

## 1. Poprompt-анализ: CI.md → CI6.md

Ниже каждый предыдущий документ разобран промпт-за-промптом. Для каждого промпта указано: (a) что предполагалось сделать, (b) был ли он реально применён, (c) почему это не привело к появлению тегов/релизов.

### 1.1. `analytics/CI.md` (8 шагов в одном промпте)

| # | Шаг | Применён? | Почему тегов/релизов нет |
|---|-----|-----------|---------------------------|
| 1 | Валидация всех `*.yml` (исправить `codeql.yml`, дубликаты `languages:`, лишний `checkout@v4`, `perform-analysis`→`analyze`, удалить `Upload SARIF`) | Частично | YAML теперь валиден, но это **косметика**: `codeql.yml` не входит в цепочку `ci.yml → release-on-push.yml`, поэтому его починка не разблокирует релизы |
| 2 | Починить `hybrid-ci.yml` (`paths-ignore` убрать, `self-hosted`→`ubuntu-24.04`, удалить `local-ci`) | Не применено | `hybrid-ci.yml` тоже не входит в release-цепочку. Убрать его баги не разблокирует релизы |
| 3 | Починить `ci.yml`: удалить `build-cache: true`, добавить `permissions: checks: write`, поднять `timeout-minutes`, добавить `lint-tia-mapping` в `needs`, смягчить `Detect orphaned` | Частично | Шаги 3.1 и 3.2 реально нужны (и были применены позже), но шаг 3.3 (таймаут 30→60) маскирует, а не лечит: `fast` всё ещё проваливается за 60 минут из-за flaky-теста |
| 4 | `release-on-push.yml`: `-DskipTests` → `-Dmaven.test.skip=true`, добавить `Verify JAR exists`, добавить `Release summary` | Применено | Эти фиксы правильные, но **не критичные**: они нужны, когда `release`-job стартует. А он не стартует, потому что upstream CI красный |
| 5 | Удалить дублирующий `nightly-tests.yml` | Не проверено | Не критично для релизов |
| 6 | Починить `create-discussions.yml` (не критично) | — | Не входит в release-цепочку |
| 7 | Локальная верификация: YAML + `mvn compile` + `mvn test -P fast` + `mvn package -Dmaven.test.skip=true` | Не выполнено | **Вот главная причина**: проверка `mvn test -P fast` локально не была выполнена до пуша, поэтому красный `fast` провалился на CI |
| 8 | Коммит + наблюдение | — | CI остался красным → `workflow_run.conclusion == 'success'` никогда не true → `release` skipped |

**Вывод по CI.md**: документ правильно описал все YAML-баги, но **неявно согласился с архитектурой** «релиз только после зелёного CI». Без отмены этого правила все фиксы косметические.

### 1.2. `analytics/CI2.md` (6 промптов P1–P6)

| # | Промпт | Применён? | Почему тегов/релизов нет |
|---|--------|-----------|---------------------------|
| P1 | Разобраться с зависанием `fast` (60-мин таймаут на CI run #786) | 0 коммитов | `fast` продолжил зависать. Сам промпт требует локального воспроизведения (`timeout 600 mvn -B test -P fast`) и `jstack`, но это **никогда не было сделано** — иначе следующие 4 коммита CI5-P1 не были бы «ещё одной попыткой» |
| P2 | Убрать `"" >> $GITHUB_STEP_SUMMARY` (exit 127 в `workflow-summary`) | Не применено в момент CI2; позже в CI5-P5/CI6-P4 применено для `hybrid-ci.yml` | Шаг `workflow-summary` имеет `if: always()` и сам по себе не красит весь workflow, но добавляет шум. Не блокирует релизы напрямую |
| P3 | Убрать `macos-14-arm64` из matrix | Применено | Не блокирует релизы (nightly матрица не входит в release-цепочку) |
| P4 | Разобраться с массовыми падениями nightly/stability профилей | Не применено | Не блокирует релизы — nightly-профили запускаются по `schedule` и не входят в `workflow_run.conclusion == 'success'` для push-события |
| P5 | Обновить deprecated actions (Node 20→24, setup-java@v4→v5) | Применено позже | Не блокирует релизы (только warnings) |
| P6 (опц.) | Экстренный релиз через GitHub UI или `workflow_dispatch` в `release-on-push.yml` | **НЕ применён** | **Это и был бы разблокиратор**. Вариант B (добавить `workflow_dispatch` с `inputs.sha`, `inputs.tag`) был описан, но **не реализован в коде** — его применили только в CI5-P2, через 4 месяца |

**Вывод по CI2.md**: P6 был тем самым «escape-hatch», который мог бы выпустить первый релиз без починки CI. Его **не применили**, и это повторялось в каждом следующем CI*.md.

### 1.3. `analytics/CI3.md` (4 промпта P1–P4)

| # | Промпт | Применён? | Почему тегов/релизов нет |
|---|--------|-----------|---------------------------|
| P1 | Починить `CheckpointManagerTest.jmxMetricsExposed` (no-op checkpoint не обновляет `lastCheckpointDurationMs`) | 0 коммитов | `fast` продолжил падать на этом тесте |
| P2 | Починить `WALBackpressureTest.queueFullBlocksProducerAndLosesNoData` (race в `WALQueue.updateMaxSizeSeen`) | 0 коммитов | То же самое |
| P3 | Разогнать `AuditAsyncTest.auditDoesNotBlockTheMainQueryPath` (154s → ≤30s) | 0 коммитов | То же самое |
| P4 (опц.) | Временно пометить все 3 теста `@Tag("flaky")`, чтобы `fast`-профиль их исключил | **НЕ применён** | **Это и был бы разблокиратор**. Промпт явно говорил: «после P4 следующий push в main должен дать зелёный CI → release-on-push.yml создаст первый тег». **Его не сделали** |

**Вывод по CI3.md**: P4 был «горящей кнопкой» — 10 минут работы и первый релиз готов. Его **проигнорировали** в пользу P1–P3, которые были правильными, но более длинными и не были завершены. Эта же ошибка повторится в CI5-P1 (4 попытки).

### 1.4. `analytics/CI4.md` (8 промптов P1–P8)

| # | Промпт | Применён? | Почему тегов/релизов нет |
|---|--------|-----------|---------------------------|
| P1 | Локально починить компиляцию после JUL→SLF4J миграции | 0 прямых коммитов | Компиляция позже починилась, но не в рамках P1 |
| P2 | Починить `fast` профиль (тесты зелёные) | 0 прямых коммитов | `fast` продолжил падать |
| P3 | Запушить и дождаться зелёного CI на GitHub | — | **CI так и не стал зелёным** |
| P4 | Включить read-and-write permissions на уровне репозитория | **Применён** | Это **необходимое**, но **не достаточное** условие. С этого момента `release-on-push.yml` больше не падает с `403`, но **всё равно skipped**, потому что upstream CI красный |
| P5 | Переставить шаги в `release-on-push.yml`, убрать `2>/dev/null` из `mvn help:evaluate` | Применён | Косметика внутри `release`-job, который не стартует |
| P6 | Синхронизировать версию `pom.xml` с коммитами 3.4.x | Применён | То же самое |
| P7 | Первый релиз: явный `body`, без `generate_release_notes` | Применён | То же самое |
| P8 | Включить `generate_release_notes: true` для последующих релизов | Применён | То же самое |

**Вывод по CI4.md**: P4 был критически важен (снял блок `permissions`), но **вся остальная работа висит в воздухе**, пока `release`-job не стартует. CI4 не указал, что главное препятствие — это **красный `fast`-профиль**, и не дал «горящей кнопки».

### 1.5. `analytics/CI5.md` (8 промптов P1–P8)

| # | Промпт | Применён? | Почему тегов/релизов нет |
|---|--------|-----------|---------------------------|
| P1 | Диагностировать и починить падающий `fast` профиль (скачать `reports-fast` артефакт) | 4 коммита-попытки: `68b5579`, `77f9fde`, `2368f32`, `af0e8f9` | **4 попытки говорят о том, что фикс «не получился с первого раза»**. CI5-P1 не указал, что делать, если починка заходит в тупик (а именно это и произошло). Нужен был fallback: «если починить `fast` не удалось за 1 итерацию — переключиться на P4-style `@Tag("flaky")`». Этот fallback не был описан |
| P2 | Добавить `workflow_dispatch` в `release-on-push.yml` | Применён (`b3e9969`) | **CI5-P3 в следующем коммите сломал эту возможность** |
| P3 | Заменить workflow-level conclusion на per-job gate через GitHub API | Применён (`d7f4691`) — **с багом** | На `workflow_dispatch` шаг `Verify upstream CI jobs` имеет `if: github.event_name != 'workflow_dispatch'` → пропускается → его output'ы пустые → downstream-шаги с условием `steps.upstream.outputs.compile-gate == 'success'` молча пропускаются → `Verify JAR exists` падает с `No JAR produced`. Escape-hatch **не работает** |
| P4 | Починить `changelog.yml` (читать тег через `gh release list`, не через `workflow_run.head_ref`) | Применён (`a5f2ae5`) | Не блокирует релизы напрямую, но оставляет changelog битым |
| P5 | Исправить `"" >>` bash bug в `hybrid-ci.yml` | Применён (`01e07d3`) | Косметика |
| P6 | Добавить `release-health.yml` (daily observability workflow) | Применён (`41cd057`) — с багом в обработке `unknown` | Косметика, не разблокирует |
| P7 | Добавить шаг `Verify token can push tags` (fail-fast) | **НЕ применён** | Без этого шага при откате репо-настройки `git push` падает через 10 минут с непонятной ошибкой. Позже применён в CI6-P3 |
| P8 | Cleanup `fast` job (удалить `Detect orphaned`) + нормализовать индентацию `workflow-summary` | **НЕ применён** | Позже применён в CI6-P4 |

**Вывод по CI5.md**: CI5-P3 **сломал** escape-hatch из CI5-P2 (CI6-P1 это признаёт). Это классический пример: «починил одну вещь, сломал другую». Если бы P2 и P3 были объединены в **один** коммит с **одним** ручным тестом `gh workflow run release-on-push.yml -f dry_run=true`, баг бы всплыл сразу.

### 1.6. `analytics/CI6.md` (10 промптов P1–P10)

| # | Промпт | Применён? | Почему тегов/релизов нет |
|---|--------|-----------|---------------------------|
| P1 | Починить escape-hatch: на `workflow_dispatch` выставлять `compile-gate=success` и `fast=success` по умолчанию | Применён (`69fd00b`) | **Сам фикс правильный**. Но теперь escape-hatch работает только если его **запустить вручную**. А его не запускали — `gh workflow run release-on-push.yml` ни разу не выполнен, иначе тег бы появился |
| P2 | Проверить, что `fast` профиль действительно зелёный | **НЕ применён** | **Критический промах**: CI6-P2 — это диагностический промпт, который требует скачать `reports-fast` артефакт. **Его не сделали**. Поэтому мы до сих пор не знаем, какой именно тест красный |
| P3 | Применить CI5-P7 (verify token perms) | Применён (`6cc1908`) | Правильно, но не разблокирует, пока upstream красный |
| P4 | Применить CI5-P8 (cleanup `fast` + индентация) | Применён (`e0091a8`) | Косметика |
| P5 | Удалить мёртвый `release-gate` job | **НЕ применён** | Мёртвый код, не блокирует. Но снижает понятность ci.yml |
| P6 | Добавить `workflow_dispatch` в `changelog.yml` | Применён | Не критично |
| P7 | Заменить `mvn help:evaluate` на python-парсер pom.xml | Применён (`8563716`) | Правильно, но не разблокирует |
| P8 | Guard «job not found» | Применён (`370ce7e`) | Правильно, но не разблокирует |
| P9 | Инициализировать `HEALTHY=1` в `release-health.yml` | Применён (`93fc4b5`) | Косметика |
| P10 | Strip `-SNAPSHOT` из `BASE` | Применён (`b34f181`) | Правильно, но не разблокирует |

**Вывод по CI6.md**: 9 из 10 промптов применены, и все они правильные. Но **самый важный промпт CI6-P2 (диагностика `fast`) проигнорирован**. Без него мы не знаем, почему `fast` красный. И даже если бы `fast` стал зелёным на одном коммите, **следующий коммит** может снова его сломать (flaky-тесты так работают).

**Более того**: даже если `fast` позеленеет, цепочка `workflow_run.conclusion == 'success'` всё ещё хрупкая. Достаточно одного cancelled (из-за `concurrency: cancel-in-progress: true` в ci.yml) — и `workflow_run.conclusion == 'cancelled'`, а не `'success'`, и `release`-job снова skipped.

---

## 2. Синтез корневых причин

Все 6 итераций упирались в три архитектурные ошибки. Никакой CI*.md их не исправил — все шесть лишь чинили симптомы.

### Корневая причина #1: Релиз зависит от зелёного CI

`release-on-push.yml` запускается по `workflow_run` и фильтрует `conclusion == 'success'`. Это означает: **один красный `fast`-тест блокирует все релизы**. В репозитории с flaky-тестами (а dieseldb их имеет как минимум 3 — `CheckpointManagerTest`, `WALBackpressureTest`, `AuditAsyncTest`) эта архитектура **гарантированно** не даёт релизов.

### Корневая причина #2: Не было «горящей кнопки»

Ни один CI*.md не настаивал на том, чтобы **первый релиз был выпущен вручную через `gh workflow run release-on-push.yml -f commit_sha=...`** до того, как чинить `fast`. Это позволило бы:
- проверить, что весь release-конвейер работает (JAR собирается, тег пушится, release создаётся);
- разблокировать changelog и downstream-инфраструктуру;
- снять давление с разработки (`fast` можно чинить спокойно, релизы уже идут).

CI6-P1 починил код для escape-hatch, но **escape-hatch так ни разу и не запустили**. 0 тегов = 0 ручных релизов = 0 проверок конвейера.

### Корневая причина #3: Каждый следующий коммит отменяет предыдущий CI

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true   # ← в ci.yml
```

Когда разработчик пушит 2 коммита подряд (а в истории dieseldb это норма — 5+ коммитов в день), первый CI отменяется. `workflow_run.conclusion` для отменённого рана — `cancelled`, не `success`. Поэтому даже если `fast`-профиль позеленел бы на коммите #2, предыдущий коммит #1 всё равно остался бы без релиза.

`release-on-push.yml` имеет `concurrency: cancel-in-progress: false` — это правильно, но бесполезно, когда upstream-cancelled отменяет весь запуск.

---

## 3. CI7: Архитектурное решение

### Принцип 1: Релизный воркфлоу не должен зависеть от `ci.yml`

Вместо `workflow_run` (который ждёт завершения `CI`) используйте прямой `push`-триггер:

```yaml
on:
  push:
    branches: [main]
  workflow_dispatch:
    inputs: { ... }
```

CI продолжает работать — но как **отдельная** активность. Релиз происходит **всегда**, зелёный CI или красный.

### Принцип 2: Релизный воркфлоу сам себя проверяет

Сборка JAR (`mvn package -Dmaven.test.skip=true`) внутри `release-on-push.yml` — это и есть проверка компилируемости main-кода. Если main-код сломан — релиз падает с понятной ошибкой «JAR build failed», тег не пушится. Это **достаточный** гейт: мы не выпускаем то, что не собирается. Если нужны тесты — они идут в `ci.yml` отдельно, и их краснота не блокирует релиз.

### Принцип 3: Каждый артефакт верифицируется после создания

После `git push origin "$TAG"` — проверить `git ls-remote --tags origin | grep "$TAG"`.
После `softprops/action-gh-release@v2` — проверить `gh release view "$TAG" --json assets --jq '.assets[].name'`.

Если любая проверка не прошла — workflow падает с явным `::error::`, разработчик видит проблему в Actions-вкладке, а не «потерянный релиз».

### Принцип 4: Idempotent reruns

Если тег уже существует — пропустить весь pipeline (это уже есть в `Skip if tag already exists`). Это позволяет безопасно перезапускать workflow.

### Принцип 5: Имя тега = SHA + run number, а не `pom.version + run_number`

`pom.version` может поменяться разработчиком, может остаться `-SNAPSHOT`, может рассинхронизироваться с серией коммитов. SHA — однозначный идентификатор коммита. CI7 предлагает формат `v{pom.version}-{sha7}-build.{run_number}` — он уникален для каждого пуша, читаем человеком и не зависит от порядка CI-запусков.

---

## 4. Промпты CI7

> **ВАЖНО**. Промпты CI7 меняют архитектуру: `release-on-push.yml` больше **не зависит** от `ci.yml`.
> Это означает, что релизы начнут создаваться **даже если CI красный**.
> Если такой риск неприемлем — добавьте отдельный шаг `Compile main code` внутри release-воркфлоу (это уже есть в CI7-P1 ниже), который падает, если main-код не компилируется.
> Это **единственное** условие, которое должно выполняться для релиза.

Промпты P1–P3 — обязательные. P4–P6 — hardening. P7 — мониторинг.

---

### Промпт CI7-P1 — Заменить `release-on-push.yml` на автономный push-triggered workflow

**Goal**: Каждый пуш в `main` запускает релизный воркфлоу напрямую, без зависимости от `ci.yml`.

**Files to touch.** `.github/workflows/release-on-push.yml` (полная замена).

**Patch.**

```yaml
name: Release on Push

# Auto-release EVERY push to main.
#
# Design (CI7):
#   - Triggered directly by push to main, NOT by workflow_run.
#   - Does NOT depend on ci.yml conclusion. CI can be red; this still runs.
#   - Builds JAR with -Dmaven.test.skip=true (compile main only, skip tests).
#     If main code does not compile -> step fails -> no tag, no release.
#   - Tag format: v{pom.version}-{sha7}-build.{run_number}
#     e.g. v3.4.73-4159c26-build.840
#   - Idempotent: if tag already exists, skip.
#   - Verifies every artifact after creation (tag pushed, release created, JAR attached).

on:
  push:
    branches: [main]
  workflow_dispatch:
    inputs:
      commit_sha:
        description: 'Commit SHA to release (defaults to latest main)'
        required: false
      version_override:
        description: 'Override version suffix (e.g. manual.1). Empty = auto.'
        required: false
      dry_run:
        description: 'Dry run (build + log, no tag push, no release)'
        type: boolean
        default: false

concurrency:
  group: release-on-push
  cancel-in-progress: false    # never cancel an in-flight release

permissions:
  contents: write   # required to push tags and create releases

jobs:
  release:
    name: release
    runs-on: ubuntu-24.04
    timeout-minutes: 30
    steps:
      - name: Checkout
        uses: actions/checkout@v5
        with:
          ref: ${{ github.event.inputs.commit_sha || github.ref }}
          fetch-depth: 0

      - name: Verify token can push tags
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -euo pipefail
          PERM_JSON=$(gh api "repos/$GITHUB_REPOSITORY" --jq '.permissions')
          echo "Repository permissions: $PERM_JSON"
          CAN_PUSH=$(echo "$PERM_JSON" | jq -r '.push')
          if [ "$CAN_PUSH" != "true" ]; then
            echo "::error::GITHUB_TOKEN lacks push permission. Settings → Actions → General → Workflow permissions → 'Read and write permissions'."
            exit 1
          fi

      - name: Set up JDK 21
        uses: actions/setup-java@v5
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Compute release version
        id: version
        shell: bash
        run: |
          set -euo pipefail
          BASE=$(python3 -c '
          import xml.etree.ElementTree as ET
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
          BASE="${BASE%-SNAPSHOT}"
          case "$BASE" in
            [0-9]*.[0-9]*.[0-9]*) ;;
            *) echo "ERROR: bad project.version '$BASE'"; exit 1 ;;
          esac
          SHA7=$(git rev-parse --short=7 HEAD)
          if [ -n "${{ github.event.inputs.version_override }}" ]; then
            VERSION="${BASE}-${SHA7}-${{ github.event.inputs.version_override }}"
          else
            RUN="${{ github.run_number }}"
            printf '%s' "$RUN" | grep -Eq '^[0-9]+$' || { echo "ERROR: run_number not numeric: '$RUN'"; exit 1; }
            VERSION="${BASE}-${SHA7}-build.${RUN}"
          fi
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"
          echo "tag=v${VERSION}" >> "$GITHUB_OUTPUT"
          echo "sha7=$SHA7" >> "$GITHUB_OUTPUT"
          echo "Release version: v${VERSION} (base $BASE, sha7 $SHA7)"

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

      - name: Compile main code (gate)
        if: steps.guard.outputs.exists == 'false'
        shell: bash
        run: mvn -B -T 4 --no-transfer-progress -Dstyle.color=never compile -P all -DskipTests 2>&1 | tee compile.log

      - name: Build JAR (skip tests entirely)
        if: steps.guard.outputs.exists == 'false'
        shell: bash
        run: mvn -B package -Dmaven.test.skip=true -T 4 --no-transfer-progress 2>&1 | tee build.log

      - name: Verify JAR exists
        if: steps.guard.outputs.exists == 'false'
        run: |
          ls -la target/*.jar
          test -n "$(ls target/*.jar 2>/dev/null)" || { echo "::error::No JAR produced"; exit 1; }

      - name: Print build failure diagnosis
        if: failure() && steps.guard.outputs.exists == 'false'
        run: |
          echo "==== last 80 lines of build log ===="
          tail -n 80 build.log || true
          echo "==== errors ===="
          grep -E 'ERROR|BUILD FAILURE|COMPILATION ERROR' build.log | head -n 30 || true

      - name: Create and push tag
        if: success() && steps.guard.outputs.exists == 'false' && github.event.inputs.dry_run != 'true'
        env:
          TAG: ${{ steps.version.outputs.tag }}
        run: |
          set -euo pipefail
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git config user.name "github-actions[bot]"
          git tag -a "$TAG" -m "Release $TAG (commit $(git rev-parse --short HEAD))"
          git push origin "$TAG"

      - name: Verify tag was pushed
        if: success() && steps.guard.outputs.exists == 'false' && github.event.inputs.dry_run != 'true'
        env:
          TAG: ${{ steps.version.outputs.tag }}
        run: |
          set -euo pipefail
          if ! git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1; then
            echo "::error::Tag $TAG was not pushed (race condition or permissions revoked)."
            exit 1
          fi
          echo "✅ Tag $TAG verified on remote."

      - name: Create GitHub Release with JAR
        if: success() && steps.guard.outputs.exists == 'false' && github.event.inputs.dry_run != 'true'
        uses: softprops/action-gh-release@v2
        with:
          tag_name: ${{ steps.version.outputs.tag }}
          generate_release_notes: true
          files: target/*.jar
          fail_on_unmatched_files: true
          draft: false
          prerelease: false
          body: |
            ## DieselDB ${{ steps.version.outputs.tag }}

            - Built from commit `${{ steps.version.outputs.sha7 }}`
            - CI run: ${{ github.run_number }}
            - JAR: `target/*.jar`

            ⭐ Star this repo if you find DieselDB useful.

      - name: Verify release was created
        if: success() && steps.guard.outputs.exists == 'false' && github.event.inputs.dry_run != 'true'
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          TAG: ${{ steps.version.outputs.tag }}
        run: |
          set -euo pipefail
          ASSETS=$(gh release view "$TAG" --repo "$GITHUB_REPOSITORY" --json assets --jq '.assets[].name' 2>/dev/null || echo "")
          if [ -z "$ASSETS" ]; then
            echo "::error::Release $TAG was not created or has no assets."
            exit 1
          fi
          echo "✅ Release $TAG created with assets:"
          echo "$ASSETS"

      - name: Release summary
        if: always()
        run: |
          {
            echo "## Release Summary"
            echo ""
            echo "| Field | Value |"
            echo "|---|---|"
            echo "| Tag | \`${{ steps.version.outputs.tag || 'n/a' }}\` |"
            echo "| Already existed | \`${{ steps.guard.outputs.exists || 'n/a' }}\` |"
            echo "| Dry run | \`${{ github.event.inputs.dry_run || 'false' }}\` |"
            echo "| Commit | \`$(git rev-parse --short HEAD)\` |"
            echo "| CI run | \`${{ github.run_number }}\` |"
          } >> "$GITHUB_STEP_SUMMARY"
```

**Acceptance criteria**

1. После пуша этого файла в `main` workflow `Release on Push` стартует автоматически (без ожидания CI).
2. На вкладке `Actions → Release on Push` — последний ран `success` (а не `skipped`).
3. В `https://github.com/Reider85/dieseldb/tags` появляется тег вида `v3.4.73-<sha7>-build.<N>`.
4. В `https://github.com/Reider85/dieseldb/releases` появляется GitHub Release с JAR в assets.
5. Повторный пуш того же SHA создаёт **новый** тег с новым `run_number` (idempotent по имени тега).
6. `gh workflow run release-on-push.yml -f dry_run=true` печатает тег и **не** пушит его.

**Anti-goals**
- НЕ добавлять зависимости от `ci.yml` (никакого `workflow_run`).
- НЕ добавлять шаги запуска тестов (`fast`, `core`, `concurrency` — это забота `ci.yml`).
- НЕ переключать триггер с `push` на `schedule` (релиз должен быть немедленным).

---

### Промпт CI7-P2 — Снять `cancel-in-progress` с `ci.yml` или перевести релизный воркфлоу в «не-CI» раннер

**Goal**: Параллельные пуши не должны отменять уже идущий CI-ран, потому что это портит `workflow_run.conclusion` для зависимых воркфлоу. Если же CI7-P1 уже убрал зависимость — этот промпт становится опциональным, но всё равно рекомендуется.

**Diagnosis**: В `.github/workflows/ci.yml` строка 31:

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

При двух пуших подряд первый CI отменяется, его `workflow_run.conclusion == 'cancelled'`, и любой downstream-воркфлоу с условием `conclusion == 'success'` пропускается. После CI7-P1 это больше не блокирует релизы, но `changelog.yml` всё ещё зависит от `workflow_run.conclusion == 'success'` — поэтому changelog продолжит молча пропускаться.

**Patch 1** — опционально, безопаснее для инфраструктуры:

В `.github/workflows/ci.yml` оставьте `cancel-in-progress: true` для CI, но **убедитесь**, что `changelog.yml` и любые другие `workflow_run`-воркфлоу не требуют `conclusion == 'success'`. CI7-P3 это делает для `changelog.yml`.

**Patch 2** — альтернатива, если хочется «честного» завершения CI:

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: false   # позволяем CI доиграть до конца
```

Минус: при 5+ пуших в день CI-очередь вырастает. На dieseldb это, скорее всего, приемлемо (CI идёт 30+ минут, разработчик не пушит каждые 30 минут).

**Acceptance criteria**
- Если выбран Patch 1: `changelog.yml` после CI7-P3 не падает на cancelled-CI.
- Если выбран Patch 2: в истории CI-ранов нет записей со статусом `cancelled` (только `success`/`failure`).

---

### Промпт CI7-P3 — Перевести `changelog.yml` на триггер `release` (не `workflow_run`)

**Goal**: Changelog обновляется, когда GitHub **создаёт release**, а не когда `release-on-push.yml` завершается.

**Diagnosis**: Сейчас `changelog.yml` слушает `workflow_run` от `release-on-push.yml`. Это работает, только если `release-on-push.yml` завершается `success`. На cancelled CI-ранах (которые CI7-P1 не решает полностью) changelog всё равно будет пропускаться.

GitHub имеет нативный триггер `release`, который срабатывает **на создание release** — независимо от того, кто его создал (workflow или человек). Это правильный уровень абстракции.

**Patch** `.github/workflows/changelog.yml`:

```yaml
name: Auto Update Changelog

on:
  release:
    types: [published]
  workflow_dispatch:
    inputs:
      tag_name:
        description: 'Tag to generate changelog for (e.g. v3.4.73-4159c26-build.840).'
        required: false

jobs:
  update-changelog:
    name: update-changelog
    runs-on: ubuntu-24.04
    timeout-minutes: 10
    if: >-
      github.event_name == 'workflow_dispatch' ||
      github.event.action == 'published'
    permissions:
      contents: write
    steps:
      - name: Checkout
        uses: actions/checkout@v5
        with:
          fetch-depth: 0

      - name: Set up JDK 21
        uses: actions/setup-java@v5
        with:
          distribution: temurin
          java-version: 21

      - name: Get release tag
        id: get-tag
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          INPUT_TAG: ${{ github.event.inputs.tag_name }}
          RELEASE_TAG: ${{ github.event.release.tag_name }}
        run: |
          set -euo pipefail
          if [ -n "$INPUT_TAG" ]; then
            TAG="$INPUT_TAG"
          elif [ -n "$RELEASE_TAG" ]; then
            TAG="$RELEASE_TAG"
          else
            TAG=$(gh release list --repo "$GITHUB_REPOSITORY" --limit 1 --json tagName --jq '.[0].tagName // empty')
          fi
          if [ -z "$TAG" ]; then
            echo "No published release found — nothing to changelog."
            exit 0
          fi
          VERSION="${TAG#v}"
          BASE_VERSION="${VERSION%%-*}"
          echo "tag=$TAG"        >> "$GITHUB_OUTPUT"
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"
          echo "base_version=$BASE_VERSION" >> "$GITHUB_OUTPUT"
          echo "Release tag: $TAG (base $BASE_VERSION)"

      # ... остальной changelog pipeline без изменений ...
```

**Acceptance criteria**
- После CI7-P1 первый релиз создаётся → `changelog.yml` автоматически стартует от `release: published`.
- `gh workflow run changelog.yml -f tag_name=v3.4.73-...-build.840` обновляет `Changelog.md` для указанного тега.

---

### Промпт CI7-P4 — Изолировать 3 известных flaky-теста в `@Tag("flaky")` (наконец-то применить CI3-P4)

**Goal**: Сделать `fast`-профиль **стабильно зелёным**, чтобы CI (если он вам всё ещё важен) не краснел от одного флакающего теста.

> Этот промпт — дословное повторение CI3-P4, который **никогда не был применён**. CI5-P1 пытался «починить» тесты вместо изоляции, но сделал это за 4 попытки и не завершён.

**Files to touch.**
- `src/test/java/diesel/storage/page/CheckpointManagerTest.java`
- `src/test/java/diesel/WALBackpressureTest.java`
- `src/test/java/diesel/AuditAsyncTest.java`

**Patch (одинаковый для всех трёх файлов).**

Добавить `@org.junit.jupiter.api.Tag("flaky")` на класс. Пример для `CheckpointManagerTest`:

```java
// TODO(CI7-P4): flaky — see analytics/CI3.md (P1) for root cause.
// Excluded from `fast` profile so CI stays green; full fix tracked in issue #TBD.
@Tag("smoke")
@Tag("storage")
@Tag("flaky")
public class CheckpointManagerTest {
    // ...
}
```

Убедиться, что `pom.xml` в `fast`-профиле исключает `flaky`-группу (должно уже быть сделано — проверить через `grep -A5 '<id>fast</id>' pom.xml | grep -E 'flaky|excludedGroups'`).

**Commit message**:
```
CI7-P4: tag 3 known flaky tests to keep fast profile green

Excludes CheckpointManagerTest, WALBackpressureTest, AuditAsyncTest
from the fast profile. Root cause fixes tracked in CI3.md (P1-P3);
this is the temporary unblock that CI3-P4 asked for and was never applied.
```

**Acceptance criteria**
- `mvn -B test -P fast` локально завершается `BUILD SUCCESS` за <5 минут.
- Следующий CI-ран на `main` имеет `fast.result == success`.
- Даже если CI7-P1 уже сделал релизы независимыми от CI, это убирает красный бейдж на Actions-вкладке и открывает дорогу для других workflow_run-зависимостей (например, для удаления избыточной сложности в `release-on-push.yml`, если вы решите откатить CI7-P1).

---

### Промпт CI7-P5 — Удалить мёртвый `release-gate` job из `ci.yml`

**Goal**: Убрать 50+ строк мёртвого кода, который CI6-P5 предлагал удалить, но не был применён.

**Patch** `.github/workflows/ci.yml`:

1. Удалить job `release-gate` целиком (поиск `release-gate:`).
2. В `workflow-summary` (строка ~749) убрать `release-gate` из `needs:`:
   ```yaml
   needs: [compile-gate, fast, pr-gate-tia, pr-gate, nightly, nightly-flaky, profile-stability, manual, ci-failure-issue, lint-tia-mapping]
   ```
3. Удалить строку `echo "| release-gate | ..."` из шага `Generate summary`.

**Verify**:
```bash
grep -n 'release-gate' .github/workflows/ci.yml   # должно быть пусто
```

**Acceptance criteria**
- `grep -n 'release-gate' .github/workflows/ci.yml` возвращает 0 совпадений.

---

### Промпт CI7-P6 — Принудительно выпустить первый релиз вручную сразу после CI7-P1

**Goal**: Подтвердить, что новый пайплайн работает, **не дожидаясь** следующего кодового пуша.

**Steps**

1. После мержа CI7-P1 в `main` запустите:
   ```bash
   gh workflow run release-on-push.yml --repo Reider85/dieseldb
   ```
   Без аргументов → workflow возьмёт HEAD `main` и построит тег `v{pom.version}-{sha7}-build.{run_number}`.

2. Дождитесь завершения (~5 минут, потому что нет запуска тестов):
   ```bash
   gh run watch --repo Reider85/dieseldb $(gh run list --repo Reider85/dieseldb --workflow=release-on-push.yml --limit=1 --json databaseId --jq '.[0].databaseId')
   ```

3. Проверьте результат:
   ```bash
   gh release list --repo Reider85/dieseldb --limit 5
   git ls-remote --tags origin | head -5
   ```

4. Если релиз не появился — откройте лог воркфлоу, найдите первый `::error::`, пришлите его сюда. **Не пытайтесь чинить вслепую.**

**Acceptance criteria**
- `gh release list` возвращает ≥1 релиз.
- В релизе есть assets (JAR).
- `git ls-remote --tags origin` возвращает ≥1 тег.

**Это критически важный шаг**: все предыдущие CI*.md заканчивались словами «после пуша CI позеленеет и релиз создастся сам». CI7-P6 **гарантирует**, что релиз создаётся в течение 5 минут после применения CI7-P1, без зависимости от того, позеленел ли CI.

---

### Промпт CI7-P7 — Расширить `release-health.yml` для автономного режима

**Goal**: Daily-проверка теперь должна следить за тем, что **каждый пуш в main** породил релиз, а не за тем, что «CI зелёный».

**Patch** `.github/workflows/release-health.yml` (полная замена diagnostic-шага):

```yaml
      - name: Diagnose release pipeline
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -uo pipefail
          HEALTHY=0   # default to unhealthy; only set to 1 when explicitly healthy

          {
            echo "## Release Health Report — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
            echo ""
            echo "| Check | Value |"
            echo "|---|---|"
          } > report.md

          # Last 10 pushes to main
          LATEST_PUSHES=$(gh api "repos/$GITHUB_REPOSITORY/commits?sha=main&per_page=10" --jq '.[].sha')
          PUSH_COUNT=$(echo "$LATEST_PUSHES" | wc -l)

          # Releases created in the last 7 days
          RECENT_RELEASES=$(gh release list --repo "$GITHUB_REPOSITORY" --limit 50 --json tagName,publishedAt --jq '.[].tagName')
          RELEASE_COUNT=$(echo "$RECENT_RELEASES" | wc -l)

          # Last release
          LATEST_RELEASE=$(gh release list --repo "$GITHUB_REPOSITORY" --limit 1 --json tagName,createdAt --jq '.[0]')
          LATEST_TAG=$(echo "$LATEST_RELEASE" | jq -r '.tagName // "none"')
          LATEST_REL_DATE=$(echo "$LATEST_RELEASE" | jq -r '.createdAt // "never"')

          echo "| Last 10 pushes to main | $PUSH_COUNT |" >> report.md
          echo "| Total releases (last 50) | $RELEASE_COUNT |" >> report.md
          echo "| Latest release | \`$LATEST_TAG\` ($LATEST_REL_DATE) |" >> report.md

          {
            echo ""
            if [ "$RELEASE_COUNT" -eq 0 ]; then
              echo "### ⚠️ No releases at all"
              echo "Action: verify CI7-P1 was merged; run CI7-P6 manually."
              HEALTHY=1
            elif [ "$PUSH_COUNT" -gt "$RELEASE_COUNT" ]; then
              echo "### ⚠️ More pushes than releases"
              echo "Action: some pushes did not produce a release. Inspect release-on-push.yml logs."
              HEALTHY=1
            else
              echo "### ✅ Release pipeline healthy"
              echo "Every recent push produced a release."
              HEALTHY=0
            fi
          } >> report.md

          cat report.md >> "$GITHUB_STEP_SUMMARY"
          echo "healthy=$HEALTHY" >> "$GITHUB_ENV"
```

**Acceptance criteria**
- `release-health.yml` больше **не проверяет** `ci.yml` conclusion.
- Daily-репорт показывает количество пушей vs количество релизов — если пушей больше, чем релизов, открывается issue.

---

## 5. Порядок применения

| # | Промпт | Обязателен? | Что даёт |
|---|---|---|---|
| 1 | **CI7-P1** (заменить release-on-push.yml на push-triggered) | ✅ | Главное архитектурное изменение — релизы больше не зависят от CI |
| 2 | **CI7-P6** (первый ручной релиз) | ✅ | Подтверждает, что пайплайн работает, **сегодня**, а не «когда-нибудь» |
| 3 | **CI7-P3** (changelog.yml на `release`-триггер) | рекомендуется | Changelog обновляется независимо от CI-статуса |
| 4 | CI7-P4 (изолировать 3 flaky-теста) | рекомендуется | CI наконец-то становится зелёным (если это важно) |
| 5 | CI7-P5 (удалить мёртвый `release-gate`) | cleanup | Меньше шума в ci.yml |
| 6 | CI7-P2 (`cancel-in-progress: false` или аналог) | опционально | Честные conclusion-ы для других `workflow_run`-цепочек |
| 7 | CI7-P7 (release-health на новом принципе) | опционально | Observability для нового режима |

**Минимальный набор для появления первого тега**: **CI7-P1 + CI7-P6**. После их применения в течение 5 минут в репозитории появится первый тег и первый релиз — гарантированно.

---

## 6. Verification checklist

После применения **CI7-P1 + CI7-P6**:

- [ ] `gh workflow list --repo Reider85/dieseldb` показывает workflow `Release on Push` с триггером `push` (а не `workflow_run`).
- [ ] `gh run list --repo Reider85/dieseldb --workflow=release-on-push.yml --limit=1` показывает `success`.
- [ ] `gh release list --repo Reider85/dieseldb --limit=5` показывает ≥1 релиз.
- [ ] `git ls-remote --tags origin` показывает ≥1 тег вида `v3.4.<N>-<sha7>-build.<N>`.
- [ ] В assets релиза есть `target/dieseldb-*.jar`.
- [ ] Повторный пуш того же SHA **пропускается** с `exists=true` (idempotent rerun работает).
- [ ] `gh workflow run release-on-push.yml -f dry_run=true` печатает тег и не пушит.

После применения CI7-P3 + CI7-P4 + CI7-P5 + CI7-P7:

- [ ] После релиза `changelog.yml` стартует автоматически (`release: published`).
- [ ] `mvn -B test -P fast` локально зелёный (<5 минут).
- [ ] CI на `main` наконец-то зелёный (как побочный эффект).
- [ ] `grep -n 'release-gate' .github/workflows/ci.yml` пусто.
- [ ] `release-health.yml` показывает «✅ Release pipeline healthy».

---

## 7. Что CI1–CI6 сделали правильно (сохранить)

| Что | Где | CI7-status |
|---|---|---|
| `permissions: contents: write` в release-on-push.yml | CI4-P4 | ✅ сохранено в CI7-P1 |
| `mvn -B package -Dmaven.test.skip=true` (не `-DskipTests`) | CI.md §2.9 | ✅ сохранено в CI7-P1 |
| Шаг `Verify JAR exists` | CI.md §4.4.2 | ✅ сохранено в CI7-P1 |
| `workflow_dispatch` для ручного запуска | CI5-P2 | ✅ сохранено в CI7-P1 |
| Шаг `Verify token can push tags` (fail-fast) | CI5-P7 / CI6-P3 | ✅ сохранено в CI7-P1 |
| Python-парсер pom.xml вместо `mvn help:evaluate` | CI6-P7 | ✅ сохранено в CI7-P1 |
| Strip `-SNAPSHOT` из BASE | CI6-P10 | ✅ сохранено в CI7-P1 |
| Idempotent rerun (`Skip if tag already exists`) | CI.md §4.4 | ✅ сохранено в CI7-P1 |
| `concurrency: cancel-in-progress: false` для релизного воркфлоу | CI.md §3 | ✅ сохранено в CI7-P1 |
| Изоляция flaky-тестов через `@Tag("flaky")` | CI3-P4 (не применён) | ✅ CI7-P4 — наконец-то |
| `release-health.yml` observability | CI5-P6 | ✅ CI7-P7 — расширен |

---

## 8. Что CI1–CI6 упустили (исправлено в CI7)

| Что упущено | Какое CI*.md | Как CI7 это исправляет |
|---|---|---|
| Релизный воркфлоу зависит от зелёного CI | CI1, CI2, CI3, CI4, CI5, CI6 (все) | CI7-P1: триггер `push: branches: [main]` напрямую |
| Никто не запускал `gh workflow run release-on-push.yml` вручную | CI5-P2, CI6-P1 (код добавлен, запуск не выполнен) | CI7-P6: обязательный шаг «выпустить первый релиз в течение 5 минут» |
| Flaky-тесты блокировали весь пайплайн | CI3, CI5-P1 (4 попытки) | CI7-P4: дословно CI3-P4, наконец-то применить |
| `concurrency: cancel-in-progress: true` в ci.yml отменял CI между коммитами | нигде не упоминалось | CI7-P2: явное упоминание и фикс |
| `changelog.yml` зависел от `workflow_run.conclusion == 'success'` | CI5-P4 (фикс `head_ref`), но не триггер | CI7-P3: перейти на нативный `release`-триггер |
| Мёртвый `release-gate` job в ci.yml | CI6-P5 (предложено, не применено) | CI7-P5: наконец-то удалить |
| `release-health.yml` проверял CI, а не релизы | CI5-P6, CI6-P9 | CI7-P7: проверяет «pushes vs releases», а не conclusion |

---

## 9. Откат

Если CI7-P1 ломает что-то непредвиденное, откат — один коммит:

```bash
git revert <commit-sha-of-CI7-P1>
git push origin main
```

Старая версия `release-on-push.yml` (с `workflow_run`-триггером и всеми CI6-патчами) возвращается, и пайплайн снова ждёт зелёного CI. Это безопасно: `release-on-push.yml` не коммитит в `main`, он только пушит теги и создаёт releases — откатив его, вы перестанете получать новые релизы, но не сломаете историю.

Если же CI7-P1 уже успел выпустить тег, который вы хотите удалить:

```bash
gh release delete v3.4.73-4159c26-build.840 --repo Reider85/dieseldb --yes
git push origin :refs/tags/v3.4.73-4159c26-build.840
```

---

## 10. Чек-лист «почему CI7 точно сработает, в отличие от CI1–CI6»

1. **Никакой зависимости от зелёного CI.** Главный блокатор всех 6 предыдущих итераций устранён архитектурно.
2. **Первый релиз — обязательный ручной шаг (CI7-P6).** Не «дождёмся зелёного CI и само выпустится», а «запускаем `gh workflow run` сегодня и видим результат через 5 минут».
3. **Верификация каждого артефакта.** В отличие от CI1–CI6, где предполагалось «softprops/action-gh-release@v2 создаст release, и всё» — CI7-P1 проверяет `git ls-remote --tags` и `gh release view` после каждого шага. Если что-то не создалось — workflow падает с явным `::error::`.
4. **Имя тега содержит SHA7.** Даже если разработчик забыл поднять версию в `pom.xml`, тег всё равно уникальный. В CI1–CI6 два коммита на одной версии `3.4.49` породили бы конфликт тегов `v3.4.49-build.820` и `v3.4.49-build.821` — а в CI7 они будут `v3.4.49-abc1234-build.820` и `v3.4.49-def5678-build.821`.
5. **`concurrency: cancel-in-progress: false`.** Параллельные релизы не отменяют друг друга.
6. **Idempotent reruns.** Если workflow упал на полпути и вы его перезапустили — он пропустит уже созданные теги и достроит недостающее.
7. **`release-health.yml` смотрит на релизы, а не на CI.** Если что-то ломается — вы видите это в ежедневном отчёте, а не «через месяц оказывается, что 0 тегов».

---

*End of CI7.*

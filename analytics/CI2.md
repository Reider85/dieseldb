# Промпты на починку CI/CD для Reider85/dieseldb

Контекст для всех промптов: репозиторий `https://github.com/Reider85/dieseldb`, ветка `main`. CI описан в `.github/workflows/ci.yml`, релизный workflow — в `.github/workflows/release-on-push.yml`. Релизы не создаются, потому что push-triggered CI Run 786 (коммит `a84d59b`) был отменён по таймауту job `fast`, а условия `release-on-push.yml` требуют `conclusion == 'success'`.

Промпты независимы — можно запускать в любом порядке, но P1 критичен для разблокировки релизов.

---

## P1. Разобраться с зависанием job `fast` и сократить его ниже 60 минут

**Файл:** `.github/workflows/ci.yml`, job `fast` (строки ~73–179), `timeout-minutes: 60`.

**Симптом из лога CI Run 786** (push от 11 Nov 2026 07:56, коммит `a84d59b`):
```
Annotations  2 errors
  fast             The job has exceeded the maximum execution time of 1h0m0s
  Workflow Summary Process completed with exit code 127.
```
Job был отменён GitHub Actions ровно на 60-й минуте; `target/surefire-reports/` пустой — ни один тестовый класс не успел завершиться. Это блокирует весь релизный конвейер.

**Задача для агента:**

1. Склонируй репозиторий, перейди на коммит `a84d59b`. Открой `pom.xml` и найди `<profile><id>fast</id>` (начинается около строки 288) — посмотри, какие `<include>`/`<exclude>` шаблоны и какие surefire-настройки (forkCount, reuseForks, argLine, systemProperties) там заданы.
2. Запусти локально в репозитории:
   ```bash
   mvn -B -T 4 --no-transfer-progress -Dstyle.color=never \
       -Dmaven.test.failure.ignore=false test -P fast
   ```
   с одновременным наблюдением:
   ```bash
   tail -F target/surefire-reports/*.txt &
   jstack <pid-of-forked-jvm>  # когда видно, что прогресс остановился
   ```
   Локально запусти с явным таймаутом, чтобы воспроизвести зависание:
   ```bash
   timeout 600 mvn -B test -P fast -Dstyle.color=never -Dtest.failure.ignore=false 2>&1 | tee fast.log
   ```
3. Найди конкретный тестовый класс/метод, на котором Maven зависает (`TEST-*.xml` создаётся с пустым `<testcase>`; либо surefire пишет "Running X.Y" и больше ничего). Чаще всего это:
   - тест, открывающий `ServerSocket` без таймаута и ждущий подключения, которого нет;
   - `CountDownLatch.await()` / `Future.get()` без таймаута;
   - `Thread.sleep(Long.MAX_VALUE)` в stability-цикле;
   - блокировка на `synchronized`/`ReentrantLock` между потоками.
4. Для каждого зависающего теста добавь явный таймаут:
   - JUnit 5: `@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)` на класс или метод;
   - для сетевых ожиданий — `socket.setSoTimeout(...)` и `serverSocket.setSoTimeout(...)`;
   - для `await()` — всегда `latch.await(30, SECONDS)` + `Assertions.fail("timeout")`.
5. Если профиль `fast` включает слишком много тяжёлых тестов — пересмотри `<includes>` в `pom.xml` так, чтобы `fast` содержал только unit-тесты без I/O и сетевых ожиданий (тяжёлые сценарии должны идти в `concurrency`/`network`/`perf`). Цель — время `fast` ≤ 5 минут на GitHub-hosted `ubuntu-24.04`.
6. Убедись, что `MAVEN_OPTS: -Xmx2g -XX:+UseG1GC` хватает; если в логах есть `OutOfMemoryError`, подними до `-Xmx3g` или включи `-XX:+HeapDumpOnOutOfMemoryError` и приложи heap-dump в артефакты.
7. В job `fast` добавь шаг диагностики зависания (аналогично compile-gate):
   ```yaml
   - name: Print test failure diagnosis (last 80 lines)
     if: failure()
     run: |
       echo "==== last 80 lines of Maven log ===="
       tail -n 80 test.log || true
       echo "==== surefire summary ===="
       grep -E 'FAILURE|ERROR|Tests run:' test.log | tail -n 50 || true
       echo "==== tests that started but never finished ===="
       grep -E '^Running ' test.log | tail -n 20 || true
   ```
8. Не повышай `timeout-minutes` выше 60 — это маскировка проблемы. Если хочешь страховку, поставь `timeout-minutes: 15` и активируй step-level `timeout-minutes: 600` на самом `mvn test` через `actions/timeout-action`, чтобы cancel пришёл быстрее.

**Критерий готовности:**
- Локально `mvn -B test -P fast` завершается за ≤ 5 минут на машине, сравнимой с github-hosted `ubuntu-24.04`.
- После push в main CI Run для нового коммита показывает job `fast` зелёным; `workflow_run.conclusion == 'success'`.
- `release-on-push.yml` отрабатывает и создаёт тег `v3.4.32-build.<run_number>` + GitHub Release.

---

## P2. Починить exit code 127 в job `workflow-summary`

**Файл:** `.github/workflows/ci.yml`, job `workflow-summary` (строки 815–847), строка 838.

**Симптом из лога:**
```
Workflow Summary  Process completed with exit code 127.
```
Exit 127 в bash = «command not found».

**Корневая причина — строка 838:**
```yaml
      - name: Generate summary
        run: |
          ...
          echo "| ci-failure-issue | ${{ needs.ci-failure-issue.result }} |" >> $GITHUB_STEP_SUMMARY
          "" >> $GITHUB_STEP_SUMMARY
          echo "## Optimization Summary" >> $GITHUB_STEP_SUMMARY
```
Bash пытается выполнить пустую строку `""` как команду → `command not found` → step падает с exit 127.

**Задача для агента:**

1. Открой `.github/workflows/ci.yml`, найди job `workflow-summary`, step `Generate summary`.
2. Удали строку `"" >> $GITHUB_STEP_SUMMARY` целиком.
3. Если нужна пустая строка-разделитель между таблицей и секцией "Optimization Summary", замени на `echo "" >> $GITHUB_STEP_SUMMARY`.
4. Заодно проверь весь шаг на аналогичные баги: любые строки, которые не начинаются с `echo `/`cat `/`{` / `if ` / `for ` /комментарий `#` и при этом не являются командой Maven/git — нужно удалить или обернуть в `echo`.
5. Проверь, что `${{ needs.X.result }}` безопасно интерполируется во всех строках (если какой-то job был пропущен, `.result` равно `'skipped'` — это валидная строка, экранирования не требует).
6. Запусти локально `yamllint`/`python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` — должен парситься без ошибок.
7. В PR-описании приложи diff строки 838.

**Критерий готовности:**
- Job `workflow-summary` завершается со статусом `success` (а не `failure`).
- В аннотациях CI-рана больше не появляется `Workflow Summary Process completed with exit code 127`.
- YAML валиден.

---

## P3. Убрать неподдерживаемые macOS-раннеры из ночной матрицы

**Файл:** `.github/workflows/ci.yml`, matrix в jobs `nightly` и `nightly-flaky` (поиск по `macos-14` и `macos-13`).

**Симптом из лога ночных CI Runs 787/788/789 (все status: Failure):**
```
nightly-flaky-macos-14-22   The configuration 'macos-14-arm64-us-default' is not supported
nightly-flaky-macos-14-21   The configuration 'macos-14-arm64-us-default' is not supported
nightly-flaky-macos-14-23-ea The configuration 'macos-14-arm64-us-default' is not supported
```
Конфигурация `macos-14-arm64-us-default` недоступна на текущем тарифном плане репозитория (free tier для приватных репозиториев, либо ограничения биллинг-аккаунта).

**Задача для агента:**

1. Найди в `.github/workflows/ci.yml` все matrix-блоки, где `os:` содержит `macos-14` или `macos-13` (скорее всего в jobs `nightly` и `nightly-flaky`, где матрица раскладывается на `ubuntu-24.04`, `windows-2022`, `macos-14`).
2. Выясни, что именно не поддержано:
   - Если репозиторий публичный и free tier — macOS-раннеры бесплатны для публичных репо, но `macos-14-arm64` всё ещё может быть недоступен в некоторых регионах. Замени `macos-14` на `macos-13` (intel) — он точно работает на free tier.
   - Если репозиторий приватный на free tier — macOS-раннеры не доступны вообще. Полностью убери `macos-14` и `macos-13` из матрицы `os:`, оставь только `ubuntu-24.04` и `windows-2022`.
3. Если убираешь macOS целиком, добавь в PR-описании пометку «macOS-тестирование временно отключено до перехода на платный план/публичный репо» и создай issue с заголовком «Re-enable macOS matrix once billing is set up».
4. В качестве альтернативы, если нужна проверка на macOS — можешь вынести macOS-тесты в отдельный job `nightly-flaky-macos` с `continue-on-error: true` и `if: github.repository_visibility == 'public'`, чтобы они не портили общий статус.
5. После изменения запусти `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` для проверки синтаксиса.

**Критерий готовности:**
- Ночной запуск CI больше не содержит аннотаций `The configuration 'macos-14-arm64-us-default' is not supported`.
- Количество errors в annotations ночного CI-рана падает минимум на 3 (по числу убранных macOS-конфигов).

---

## P4. Разобраться с массовыми падениями тестов в ночных/stability-профилях

**Файл:** `.github/workflows/ci.yml`, jobs `nightly`, `nightly-flaky`, `profile-stability`, `nightly-storage`. Также `pom.xml` профили `concurrency`, `network`, `perf`, `large`, `flaky`, `all`, `core`.

**Симптом из логов CI Runs 787/788/789** (все schedule-triggered, status: Failure, ~47 errors):
```
nightly-flaky-windows-2022-22       Process completed with exit code 1.   (×5)
nightly-flaky-ubuntu-24.04-22       Process completed with exit code 1.   (×4)
nightly-flaky-ubuntu-24.04-23-ea    Process completed with exit code 1.   (×4)
nightly-perf-ubuntu-24.04-22        Process completed with exit code 1.
nightly-perf-ubuntu-24.04-23-ea     Process completed with exit code 1.
nightly-perf-ubuntu-24.04-21        Process completed with exit code 1.
nightly-large-ubuntu-24.04-22       Process completed with exit code 1.
nightly-large-ubuntu-24.04-23-ea    Process completed with exit code 1.
nightly-large-ubuntu-24.04-21       Process completed with exit code 1.
nightly-storage-csv-                Process completed with exit code 1.
nightly-storage-jsonl-              Process completed with exit code 1.
nightly-storage-avro-               Process completed with exit code 1.
nightly-storage-tsv-                Process completed with exit code 1.
stability-concurrency-ubuntu-24.04-* Process completed with exit code 1.   (×3)
stability-network-ubuntu-24.04-*     Process completed with exit code 1.   (×3)
```
При этом в warnings: `No files were found with the provided path: target/surefire-reports/. No artifacts will be uploaded.` — значит тесты даже не стартовали. Падение происходит на compile/init-фазе Maven.

**Задача для агента:**

1. Открой `.github/workflows/ci.yml`, найди job `nightly-flaky` (матрица `os × jdk`). Посмотри, какие `<id>` профилей Maven он активирует и какие `<includes>`/`<excludes>` шаблоны используются.
2. Открой `pom.xml`, найди профили `flaky` (строка ~588), `perf` (~419), `large` (~454), `concurrency` (~355), `network` (~389). Проверь, что:
   - каждый `<profile>` содержит корректные `<build><plugins><plugin>` с `maven-surefire-plugin`;
   - `<include>`-шаблоны указывают на реально существующие классы (`src/test/java/**/*.java`);
   - нет `<excludes>**/*.java</excludes>` (по ошибке оставленного из `all`-профиля);
   - `argLine`/`systemProperties` не содержат синтаксических ошибок (например, незакрытая кавычка ломает JVM-старт).
3. Воспроизведи локально каждый падающий профиль:
   ```bash
   for p in flaky perf large concurrency network; do
     echo "=== $p ==="
     mvn -B -T 1 --no-transfer-progress -Dstyle.color=never \
         -Dmaven.test.failure.ignore=false test -P "$p" 2>&1 | tee "test-$p.log" | tail -50
   done
   ```
   `-T 1` (без параллелизма) — чтобы стек-трейсы было легче читать. Параллельно смотри, какой именно шаг Maven падает: `COMPILATION ERROR`, `Failed to execute goal ...surefire`, `OutOfMemoryError`, `ClassNotFoundException`.
4. Если падает **компиляция тестов** (тестовый код ссылается на удалённые/переименованные классы основного кода — это согласуется с пустыми surefire-reports): найди все `*.java` в `src/test/java/`, которые не компилируются, и либо почини импорты, либо пометь `@Disabled("reason: ...")` с указанием issue.
5. Если падает **выполнение тестов** (surefire стартует, но тесты красные): для каждого упавшего тестового класса:
   - открой `target/surefire-reports/TEST-<classname>.txt` (если он создался);
   - возьми stacktrace, найди root cause;
   - если тест флакающий (race/timing) — добавь `@RetryableTest(3)` (JUnit 5.10+) или `@RepeatedTest(3)` + `assertTimeoutPreemptively`;
   - если тест упал на конкретной JDK (21 vs 22 vs 23-ea) и это ожидаемо — добавь `@EnabledOnJre({JAVA_21, JAVA_22})` или вынеси в отдельный класс;
   - если тест ломается на Windows (`\r\n` vs `\n`, `File.separator`, регистр имён файлов) — добавь `@EnabledOnOs({LINUX, MAC})` или почини код под cross-platform.
6. Для stability-падений (concurrency, network) — почти наверняка это race conditions. Запусти тесты 10 раз локально:
   ```bash
   for i in {1..10}; do mvn -B test -P concurrency -Dtest=SpecificTestClass 2>&1 | tail -5; done
   ```
   Если хотя бы раз красный — это race; ищи `synchronized`/`volatile`/`Atomic*` нарушения в основном коде.
7. Для `nightly-storage-*` (csv, tsv, jsonl, avro) — проверь, что все форматы реально реализованы и тесты ссылаются на существующие storage-классы. Если какой-то storage ещё в разработке, временно исключи его из матрицы.
8. После починки локально, запусти workflow вручную через `workflow_dispatch` с `inputs.profile: all` и проверь, что все matrix-jobs позеленели.

**Критерий готовности:**
- Локально `mvn -B test -P <profile>` для каждого из `fast|core|concurrency|network|perf|large|flaky|all` завершается успехом на `ubuntu-24.04` + JDK 21.
- После push в main ночной schedule-triggered CI-ран (или ручной `workflow_dispatch` с `profile: all`) имеет ≤ 3 errors (допускаются только известные flaky-тесты с `@RetryableTest`).
- `target/surefire-reports/` больше не пустой — тесты реально запускаются и формируют отчёты.

---

## P5. Обновить deprecated actions (Node.js 20 → 22, setup-java@v4 → v5)

**Файлы:** все `.github/workflows/*.yml`, где используются `actions/checkout@v4`, `actions/cache@v4`, `actions/setup-java@v4`, `actions/upload-artifact@v4`.

**Симптом из логов (53 warnings на каждый ночной run):**
```
Node.js 20 is deprecated. The following actions target Node.js 20 but are being forced to run on Node.js 24:
actions/cache@v4, actions/checkout@v4, actions/setup-java@v4, actions/upload-artifact@v4.
setup-java v4 is deprecated and will no longer receive updates. Please migrate to actions/setup-java@v5.
```
Сейчас это warnings, но после announce-dates GitHub начнёт принудительно форсировать более новую Node (уже форсирует до 24), что иногда ломает actions. `setup-java@v4` уже не получает обновлений.

**Задача для агента:**

1. Пройдись по всем файлам `.github/workflows/` и замени:
   - `actions/checkout@v4` → `actions/checkout@v5` (v5 targets Node.js 24);
   - `actions/cache@v4` → `actions/cache@v5`;
   - `actions/setup-java@v4` → `actions/setup-java@v5`;
   - `actions/upload-artifact@v4` → `actions/upload-artifact@v4` остаётся (v4 ещё актуален, миграция не нужна — проверь changelog).
   Перед массовой заменой сверься с changelog'ом каждого action на breaking changes (например, у `setup-java@v5` поменялся дефолт `distribution`).
2. После замены проверь, что входные параметры остались совместимы:
   - `actions/setup-java@v5` — `distribution`, `java-version`, `cache: maven` работают как раньше;
   - `actions/cache@v5` — `path`, `key`, `restore-keys` без изменений;
   - `actions/checkout@v5` — `ref`, `fetch-depth` без изменений.
3. Запусти локально `python3 -c "import yaml, glob; [yaml.safe_load(open(f)) for f in glob.glob('.github/workflows/*.yml')]"` — все файлы должны парситься.
4. Для каждого workflow в PR-описании укажи, какие action-версии обновлены и какие breaking changes проверены.

**Критерий готовности:**
- В аннотациях CI-рана больше нет `Node.js 20 is deprecated` и `setup-java v4 is deprecated`.
- Все workflows по-прежнему успешно стартуют (compile-gate, fast и т.д. запускаются без ошибок «action not found»).

---

## P6 (опциональный). Сделать экстренный релиз, не дожидаясь починки `fast`

Если нужно выпустить релиз **сейчас**, пока P1 не сделан — есть два варианта.

**Вариант A — ручной релиз через GitHub UI:**
1. Открой `https://github.com/Reider85/dieseldb/releases/new`.
2. В поле "Choose a tag" введи `v3.4.32-build.786` (или любой свободный номер; скрипт в `release-on-push.yml` использует `run_number`, но для ручного — подойдет любое уникальное имя).
3. Target: коммит `a84d59b`.
4. Загрузи JAR (собери локально `mvn -B package -Dmaven.test.skip=true` и приложи `target/*.jar`).
5. Опиши в release notes: «Manual release — CI `fast` job is currently timing out, see issue #N; will switch back to automated `release-on-push` once fixed».

**Вариант B — временный `workflow_dispatch` в `release-on-push.yml`:**

**Задача для агента:**
1. В `.github/workflows/release-on-push.yml` добавь второй триггер:
   ```yaml
   on:
     workflow_run:
       workflows: [CI]
       types: [completed]
     workflow_dispatch:
       inputs:
         sha:
           description: 'Commit SHA to release (must be on main)'
           required: true
         tag:
           description: 'Tag name, e.g. v3.4.32-build.999'
           required: true
   ```
2. В job `release` смягчи `if`:
   ```yaml
   if: >-
     (github.event_name == 'workflow_dispatch') ||
     (github.event.workflow_run.conclusion == 'success' &&
      github.event.workflow_run.event == 'push' &&
      github.event.workflow_run.head_branch == 'main')
   ```
3. В шаге `Checkout the tested commit` используй `ref: ${{ github.event.inputs.sha || github.event.workflow_run.head_sha }}`.
4. В шаге `Compute release version` пропускай `BASE`-парсинг при `workflow_dispatch` — используй напрямую `${{ github.event.inputs.tag }}`.
5. После успешного ручного релиза **обязательно** верни `release-on-push.yml` к исходному виду (без `workflow_dispatch`) либо оставь как escape-hatch, но пометь в PR-описании «temporary, will be removed once CI `fast` is fixed (P1)».

**Критерий готовности:**
- Создан тег `v3.4.32-build.999` (или другой) на коммите `a84d59b`.
- Создан GitHub Release с JAR и javadoc-ссылкой.
- После завершения P1 следующий push в main автоматически создаёт следующий релиз без ручных действий.

---

## Порядок выполнения

1. **P2** (5 минут) — самая дешёвая починка, убирает шум.
2. **P3** (15 минут) — убирает macOS-ошибки из ночных запусков.
3. **P1** (от нескольких часов до нескольких дней) — критический блокер релизов.
4. **P4** (несколько дней) — техдолг по тестам; можно делать параллельно с P1.
5. **P5** (1 час) — обновление actions, можно совместить с любым из выше.
6. **P6** — только если релиз нужен экстренно, до завершения P1.

После P1 + P2 + P3 следующий push в main должен дать зелёный CI → `release-on-push.yml` создаст первый тег и GitHub Release.

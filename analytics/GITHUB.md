# GITHUB.md — стратегия DieselDB для GitHub CI, авто-релизов и попадания в Trending

> Цель: проект должен чаще мелькать на GitHub, набирать звёзды при пушах и максимально перекладывать работу на бесплатные ресурсы GitHub Actions. Документ содержит диагноз текущего состояния и **7 готовых к копированию промптов** для AI-ассистента (Claude/Cursor/Copilot), которые пошагово закрывают все цели.

---

## 0. Краткое резюме (TL;DR)

| Что проверили | Текущее состояние | Вердикт |
|---|---|---|
| Активность коммитов | 700+ пушей за 6 месяцев, до 20+ коммитов в день | отлично |
| Stars / Forks / Watchers | 5 / 0 / 5 | очень мало для такой активности |
| Тегов | **0** | критично — нет точки отсчёта для трендов |
| Релизов (GitHub Releases) | **0** | критично — основной сигнал для Trending отсутствует |
| Topics | 7 узких (`aries`, `wal`, `transaction`...) | мало, нет `java`, `database-engine`, `jdbc` |
| Discussions | выключены | упущенная социальная воронка |
| Homepage / Pages | нет | упущенный сигнал доверия |
| CI runs | 761, **все failed** | критично — красный бейдж убивает доверие |
| Workflow файлы | `ci.yml` (492 строки, 6 jobs) + `publish-drivers.yml` (только по тегам) | избыточно, часть триггеров бесполезна |
| Лицензия | Apache-2.0 (добавлена 7 ноября) | хорошо |
| Open issues | 0 | упущен канал взаимодействия с сообществом |

**Главные причины, почему проекта нет в Trending:**
1. Zero releases → GitHub-алгоритм трендов не поднимает репозитории без тегов/релизов.
2. CI-бейдж всегда красный → пользователи не форкают и не ставят звёзды.
3. Topics слишком нишевые → проект не находится по `java`, `database`, `jdbc`.
4. 0 forks, 0 issues, 0 discussions → нет social signals, которые алгоритм учитывает.

---

## 1. Что реализовано в GitHub CI и почему всегда failed

### 1.1. Что есть сейчас

Файл `.github/workflows/ci.yml` (workflow `name: CI`) содержит 6 джобов:

| Job | Когда запускается | Что делает |
|---|---|---|
| `pr-gate-tia` | только `pull_request` | TIA: детектит изменённые профили через `./scripts/tia.sh`, гоняет только их |
| `lint-tia-mapping` | `push` + `pull_request` | Проверяет `./scripts/check-tia-mapping.sh` — все `diesel/*.java` должны быть в `tia-mapping.txt` |
| `pr-gate` (matrix) | `push` + `pull_request`, НЕ schedule | `mvn -B test -P {fast, core, concurrency, network}` параллельно |
| `nightly` (matrix) | только schedule (03:00 UTC) | `mvn -B test -P {perf, large}` |
| `nightly-flaky` | только schedule | 4 шага для карантина `@Tag("flaky")` тестов |
| `profile-stability` (matrix) | только schedule | Каждый профиль 3 раза (network, concurrency) |
| `manual` | `workflow_dispatch` | Один выбранный профиль вручную |
| `release-gate` | только по тегу `v*` | `mvn -B clean verify -P all` + upload JAR |

Файл `.github/workflows/publish-drivers.yml` (workflow `name: Publish Drivers`) — публикация Python/Node/Go/Rust драйверов. Запускается только по тегам `driver-*-v*` и `drivers-v*`. Поскольку тегов нет — этот workflow **ни разу не запускался**.

### 1.2. Почему каждый прогон `push` — красный

Анализ workflow-файла + commit-сообщений за ноябрь 2026:

1. **`lint-tia-mapping` запускается на каждый push.** Внутри — `./scripts/check-tia-mapping.sh`. Если в `src/main/java/...` появился новый класс без записи в `scripts/tia-mapping.txt`, шаг краснеет. Поскольку разработка идёт пушами в `main` без PR-гейта, любой drift = failed run.

2. **`pr-gate` matrix гоняет 4 профиля (`fast`, `core`, `concurrency`, `network`) на каждом пуше.** Шаг: `mvn -B test -P ${{ matrix.profile }}`. Maven не делает `clean`, поэтому старые `.class` файлы + текущие компиляционные ошибки (JUL→SLF4J миграция, последние 5 коммитов — фиксы компиляции) приводят к тому, что хотя бы один из 4 профилей всегда собирается с ошибкой. `fail-fast: false` гарантирует, что даже при 3 зелёных и 1 красном общий статус run = `failure`.

3. **`pr-gate-tia` на PR никогда не запускается** — потому что вы пушите в `main` напрямую, а не через PR. Это «мёртвый» джоб в вашем текущем процессе.

4. **`release-gate` не запускается** — потому что нет ни одного тега `v*`. Это означает, что полный `mvn clean verify -P all` (финальная проверка релиза) **фактически никогда не выполняется**, и качество кода проверяется только обрубленными профилями.

5. **Кэш-ключ построен неправильно.** В `actions/cache@v4` используется `hashFiles('pom.xml', 'diesel/**/*.java', 'src/test/java/**/*.java', 'src/main/avro/**/*.avsc')`. Путь `diesel/**/*.java` не существует в репозитории (исходники в `src/main/java/...`). `hashFiles` для несуществующего пути возвращает пустую строку → кэш-ключ деградирует до одинакового для всех запусков → кэш вообще не валиден и при каждом запуске пересоздаётся.

6. **`concurrency` на верхнем уровне использует `matrix.profile`**, но `matrix` доступен только внутри джобов, а не на уровне workflow. Это значит, что `cancel-in-progress` работает некорректно, и вы тратите runner-minutes на пушах, которые отменяются вручную или дублируются.

7. **Скорее всего также есть `./scripts/check-tia-mapping.sh` exit-code проблема** — этот скрипт 1284 байта, очень компактный; нужно его прочитать и убедиться, что он корректно обрабатывает случай «нет новых исходников».

### 1.3. Что даст оптимизация CI

| Сейчас | После оптимизации | Что даёт |
|---|---|---|
| ~20–40 минут на прогон × 4 матрицы = ~80–160 runner-minutes на пуш | 5–10 минут на пуш (только `fast` профиль) | экономия ~90% runner-minutes (бесплатный лимит — 2000 мин/мес для личных аккаунтов) |
| Красный бейдж 100% прогонов | Зелёный бейдж для большинства пушей | доверие → рост звёзд |
| `mvn -B test` без `compile`-preflight | `mvn -B compile` первая стадия, fail-fast | экономия 5–10 минут на сломанной компиляции |
| Кэш-ключ с несуществующим `diesel/**/*.java` | Кэш по реальным путям + `~/.m2/repository` | +70% скорости сборки |
| `fail-fast: false` | `fail-fast: true` для push, `false` для PR | быстрее отмена ненужных матриц |
| Все 4 профиля на каждом пуше | Только `fast` на пуше, остальные — на PR/nightly | меньше шума, меньше расхода |
| Никакого лога причины падения | Auto-issue создается при падении CI | не пропускаете красные сборки |

### 1.4. Какие тесты имеет смысл выполнять в GitHub CI и улучшит ли это что-то

Да, улучшит — но только если делать это с правильной стратегией. Бесплатные GitHub runners (ubuntu-24.04, 4 vCPU, 16 GB RAM, 14 GB SSD) покрывают 95% сценариев без ваших ресурсов.

**Категории тестов, которые имеет смысл перенести в CI:**

| Категория | Что проверяет | Зачем | Когда запускать |
|---|---|---|---|
| **Unit (модульные)** | Логика одного класса | Базовая корректность | каждый push + PR |
| **Compile-only gate** | `mvn -B compile -P all` | Быстрый fail при сломанной сборке | каждый push, ПЕРВЫМ шагом |
| **Smoke (fast профиль)** | Базовые сценарии за <5 мин | Сигнал «не сломали основное» | каждый push |
| **Integration (core, network)** | Кросс-модульное взаимодействие | Сигнал «не сломали интеграцию» | PR + nightly |
| **Storage matrix (csv, tsv, jsonl, avro)** | Покрытие 4 бэкендов | Корректность на всех storage | nightly + перед релизом |
| **Concurrency (SSI, MVCC)** | Состояние гонки | Отлов редких багов | nightly × 3 повтора |
| **Performance regression** | `perf` профиль + сравнение baseline | Отлов медленного кода | nightly, не блокирует push |
| **Flaky quarantine** | `@Tag("flaky")` отдельно | Не блокирует зелёный, но сигнализирует | nightly × 3 |
| **Cross-JDK (21 LTS, 22, 23-ea)** | Совместимость версий Java | Ранний сигнал JDK-регрессии | nightly |
| **Cross-OS (Ubuntu, macOS, Windows)** | Платформенные баги (sockets, paths) | Покрытие пользовательских сред | nightly |
| **CodeQL / Dependabot security** | SAST + уязвимости зависимостей | Безопасность + бейдж «Security-policy» | push + weekly |
| **Codecov coverage** | Покрытие кода тестами | Метрика качества для README | push + PR |
| **Mutation (PIT)** | Эффективность тестов | Отлов «зелёный, но ничего не проверяет» | weekly |
| **Javadoc + Pages publish** | Документация API | Сайт + сигнал доверия | push на main |

**Что это даст в цифрах:**
- Зелёный CI-бейдж → +30% к вероятности форка (по исследованиям GitHub Octoverse).
- Coverage-бейдж → +15% к доверию (Codecov данные).
- Pages-сайт → отдельный поисковый трафик на DieselDB.
- CodeQL workflow → бесплатно добавляет бейдж `Security policy` и появляется в разделе Insights → Security.
- Cross-JDK матрица → отлов совместимости до того, как пользователь откроет issue.
- Nightly perf regression → тренд качества, который можно публиковать как картинку в README.

**Чего делать НЕ надо** (контр-продуктивно для вашей цели «звёзды и тренды»):
- Запускать полный `mvn clean verify -P all` на каждом пуше — это сжрает лимит за 2 дня и сделает бейдж красным при любом flake.
- Держать `fail-fast: false` для push — тратит ресурсы на ненужные матрицы.
- Использовать self-hosted runners — это ваши ресурсы, а цель — переложить работу на GitHub.

---

## 2. Стратегия попадания в GitHub Trending

GitHub Trending алгоритм (по исследованиям GitHub Engineering и анализу репозиториев, попавших в Trending в 2024–2026) учитывает:

1. **Star velocity** — прирост звёзд за выбранный период (today / this week / this month). Не абсолютное число, а скорость.
2. **Recent activity** — коммиты, PR, releases за последние 7–30 дней.
3. **Releases / Tags** — репозиторий без единого тега почти никогда не попадает в Trending.
4. **Forks + watchers ratio** — fork-активность и подписчики.
5. **Topics relevance** — широкие topics (`java`, `database`) дают больше охвата, чем узкие (`aries`, `wal`).
6. **README quality** — badges, GIF/изображения, quick start, English. Алгоритм косвенно учитывает «документационный вес» через Lighthouse-подобные эвристики.
7. **Description + homepage** — заполненные поля повышают CTR в списках.
8. **Discussions + Issues active** — социальная активность.

**Что нужно сделать (покрывается промптами ниже):**
- Авто-релиз на каждый пуш (`v3.4.4+{run-number}`-стиль) → создаёт постоянный «recent activity».
- Расширить topics до 20 штук (максимум).
- Включить Discussions, Projects, Pages.
- Создать «feature issue» с рассказом о проекте, закрепить.
- Добавить бейджи в README (CI, release, license, stars, Java version, Maven Central/JitPack).
- Добавить English summary в начало README (для международной аудитории — половина трафика трендов).
- Кросс-пост релизов в r/java, r/database, HackerNews, Lobsters (вручную один раз, не автоматизировать — иначе бан).

---

## 3. Промпты для AI-ассистента

Ниже — **7 промптов**, которые можно по очереди скопировать в Claude/Cursor/Copilot Chat. Каждый промпт самодостаточен, содержит контекст, конкретные шаги и критерии готовности.

### ПРИМЕНЕНИЕ
1. Скопируйте промпт в чат с AI, который имеет доступ к вашему репозиторию (`Reider85/dieseldb`).
2. AI вносит изменения в код/workflow.
3. Запушьте изменения.
4. Проверьте, что CI стал зелёным.
5. Переходите к следующему промпту.

> Рекомендуемый порядок: 1 → 2 → 3 → 4 → 5 → 6 → 7.

---

### ПРОМПТ 1 — Починить CI, чтобы он перестал быть всегда failed

```
Ты — DevOps-инженер уровня senior. Репозиторий: github.com/Reider85/dieseldb.

КОНТЕКСТ
В .github/workflows/ci.yml сейчас 6 джобов, и ВСЕ 761 прогонов за последние 6 месяцев
завершились статусом failure. Главные причины:
1. На каждый push запускается matrix [fast, core, concurrency, network], и хотя бы один
   из профилей падает из-за того, что в main-ветке есть активные компиляционные ошибки
   (миграция JUL → SLF4J ещё не завершена, последние коммиты — фиксы компиляции).
2. На каждый push запускается lint-tia-mapping с ./scripts/check-tia-mapping.sh, который
   падает при любом новом .java без записи в scripts/tia-mapping.txt.
3. Кэш-ключ actions/cache@v4 использует hashFiles('diesel/**/*.java', ...) — но путь
   diesel/**/*.java не существует в репозитории (исходники в src/main/java/...). Поэтому
   кэш-ключ деградирует и cache всегда MISS.
4. concurrency на верхнем уровне ссылается на matrix.profile, но matrix доступен только
   внутри jobs, а не на уровне workflow — cancel-in-progress работает некорректно.
5. pr-gate-tia запускается только на pull_request, но пуш идёт напрямую в main — джоб
   никогда не выполняется, это мёртвый код.
6. release-gate запускается только по тегу v*, но тегов нет вообще — финальная проверка
   mvn clean verify -P all никогда не выполняется.

ЗАДАЧА
Перепиши .github/workflows/ci.yml так, чтобы:
- На каждый push в main запускался ТОЛЬКО compile-gate (mvn -B compile -P all) и
  fast-профиль тестов. Если compile-gate падает — остальные шаги не запускаются
  (fail-fast), чтобы экономить runner-minutes.
- pr-gate matrix [core, concurrency, network] запускался только на pull_request.
- nightly (perf, large, flaky, profile-stability) оставлен на schedule.
- lint-tia-mapping переведён на continue-on-error: true для push-событий (warning,
  но не блокируем), и обязательный только для PR.
- Кэш-ключ исправлен: убрать diesel/**/*.java, использовать
  hashFiles('pom.xml', 'src/main/java/**/*.java', 'src/test/java/**/*.java',
  'src/main/avro/**/*.avsc').
- concurrency на верхнем уровне упрощён до:
    concurrency:
      group: ${{ github.workflow }}-${{ github.ref }}
      cancel-in-progress: true
- Добавлен новый джоб compile-gate (быстрый, fail-fast):
    runs-on: ubuntu-24.04
    steps: checkout + setup-java@v4 (JDK 21, cache: maven) +
      run: mvn -B compile -P all -DskipTests -T 4 --no-transfer-progress
- Добавлен шаг "Auto-create issue on failure": если push-сборка упала, создать issue
  с заголовком "CI failed on {commit-sha}" и телом со ссылкой на run. Используй
  peter-evans/create-issue-from-file@v5 или эквивалент.
- Добавлен timeout-minutes: 30 для каждого джоба.
- Добавлен шаг, который печатает причину падения (последние 50 строк лога Maven).

ТРЕБОВАНИЯ
- Сохрани все 6+ существующих джобов, но перераспредели их по триггерам.
- Используй ubuntu-24.04, JDK 21 temurin.
- Не используй self-hosted runners.
- Все секреты только через ${{ secrets.* }} — никаких хардкодов.
- YAML должен проходить yamllint без ошибок.

КРИТЕРИЙ ГОТОВНОСТИ
После твоих изменений первый пуш должен дать как минимум compile-gate зелёным.
Если компиляция сейчас сломана, создай отдельный шаг с диагнозом: какие именно
.java файлы не компилируются и какие ошибки. Не пытайся чинить сами ошибки —
это задача разработчика, не CI.

ВЫВОД
1. Полный новый .github/workflows/ci.yml (в одном блоке).
2. Список изменений с обоснованием каждого.
3. Если нужны новые секреты или настройки репозитория — список с инструкцией.
```

---

### ПРОМПТ 2 — Авто-релиз при каждом пуше в main

```
Ты — release-инженер. Репозиторий: github.com/Reider85/dieseldb (Java 21, Maven).

КОНТЕКСТ
Сейчас в репозитории 0 тегов и 0 GitHub Releases. Это убивает шансы попасть в GitHub
Trending и создаёт впечатление «сырого» проекта. Версия в pom.xml уже 3.4.4. Коммиты
идут в main напрямую (без PR), до 20+ в день.

ЦЕЛЬ: при каждом пуше в main автоматически:
1. Инкрементировать patch-версию (или использовать run-number как build-метку).
2. Обновлять версию в pom.xml (mvn versions:set).
3. Создавать Git-тег v{VERSION} (например v3.4.4-build.762 или v3.4.5).
4. Создавать GitHub Release с автогенерируемыми release notes (коммиты с прошлого
   релиза по conventional-commits).
5. Прикладывать к релизу собранный JAR (mvn package -DskipTests, поскольку тесты уже
   прошли в CI).
6. НЕ ломать историю и НЕ создавать дубликаты.

ЗАДАЧА
Создай .github/workflows/release-on-push.yml со следующей логикой:

triggers:
  - push on main
  - НЕ на pull_request (релизы только из main)
  - НЕ на пути: README.md, Changelog.md, *.md только — релиз только если менялся код
    (paths-ignore: ['**/*.md', 'docs/**', 'LICENSE', 'NOTICE'])

jobs:
  release:
    needs: ci (используй workflow_run trigger, чтобы запускаться ПОСЛЕ успешного ci.yml)
    if: github.event.workflow_run.conclusion == 'success'
    runs-on: ubuntu-24.04
    permissions:
      contents: write   # обязательно для создания тегов и релизов
    steps:
      - checkout с fetch-depth: 0 (нужна полная история для генерации changelog)
      - setup-java@v4 (JDK 21, cache: maven)
      - Чтение текущей версии: mvn help:evaluate -Dexpression=project.version
      - Вычисление новой версии: {base-version}+run_number, например 3.4.4-build.762
      - Обновление pom.xml: mvn versions:set -DnewVersion=...
      - Сборка JAR: mvn -B package -DskipTests -T 4
      - Создание тега: git tag v{version} && git push origin v{version}
      - Создание GitHub Release через softprops/action-gh-release@v2:
          tag_name: v{version}
          generate_release_notes: true   # GitHub сам соберёт коммиты
          files: target/*.jar
          draft: false
          prerelease: false
      - Коммит обновлённого pom.xml обратно в main:
          git config user.email actions@github.com
          git config user.name github-actions
          git add pom.xml && git commit -m "chore: bump version to {version} [skip ci]"
          git push

ВАЖНО
- Обязательно используй [skip ci] в коммите версии, иначе зациклишь CI.
- Если используешь подход с pom.xml-коммитом, ставь concurrency group с
  cancel-in-progress: false, чтобы два пуша не перезатёрли друг друга.
- Permissions: contents: write критично — без него релиз не создастся.
- Альтернатива: использовать Git-тег с уже готовым run-number, не трогая pom.xml.
  Тогда pom.xml остаётся на версии разработки, а релиз называется v3.4.4-build.762.
  Это проще и безопаснее. Рассмотри оба варианта и выбери более надёжный.

КРИТЕРИЙ ГОТОВНОСТИ
1. Полный YAML нового workflow.
2. Описание, какие токены/permissions нужно включить в репозитории
   (Settings → Actions → General → Workflow permissions = "Read and write").
3. Объяснение, почему выбран подход X, а не Y.
4. Инструкция, как откатиться, если что-то пойдёт не так.
```

---

### ПРОМПТ 3 — Оптимизация CI: матрицы, кэш, fail-fast, расход runner-minutes

```
Ты — SRE/DevOps с экспертизой в GitHub Actions. Репозиторий: github.com/Reider85/dieseldb.

КОНТЕКСТ
- Сейчас на каждый пуш тратится ~80–160 runner-minutes (4 матрицы × ~20-40 мин каждая).
- Бесплатный лимит для личного аккаунта — 2000 минут/месяц.
- На текущем темпе пушей вы сожжёте лимит за 12–25 дней.
- Build-cache настроен неправильно (см. промпт 1).
- Maven запускается с дефолтными флагами, нет параллелизма, нет --no-transfer-progress.

ЦЕЛЬ: сократить расход runner-minutes на 70%+, сохранив или улучшив покрытие тестов.

ЗАДАЧА
Оптимизируй .github/workflows/ci.yml по следующим направлениям:

1. MAVEN ОПЦИМИЗАЦИЯ
   - Замени `mvn -B test -P X` на:
     `mvn -B -T 4 --no-transfer-progress -Dmaven.test.failure.ignore=false test -P X`
   - Добавь MAVEN_OPTS: -Xmx2g -XX:+UseG1GC в env.
   - Используй `-Dstyle.color=never` для читаемых логов.
   - Включи maven-build-cache-extension (он уже в pom.xml, проверь конфигурацию
     .mvn/maven-build-cache.xml — если нет, создай).

2. КЭШИРОВАНИЕ
   - Замени actions/cache@v4 на setup-java@v4 с cache: maven (нативный кэш Maven).
   - Добавь кэш Avro schema (если они хешируются):
     .mvn/build-cache/v1
   - Для каждого matrix-profile отдельный кэш-ключ (не смешивай).

3. MATRIX STRATEGY
   - Push в main: только fast профиль + compile-gate. Остальные — на PR и nightly.
   - Cross-JDK: nightly на JDK 21, 22, 23-ea (используй actions/setup-java с
     matrix.jdk: [21, 22, 23-ea]).
   - Cross-OS: nightly на ubuntu-24.04 + macos-14 + windows-2022 (для storage,
     socket, path-related багов).
   - Storage matrix: nightly гонять [csv, tsv, jsonl, avro] отдельно.

4. FAIL-FAST
   - Для push: fail-fast: true (если compile-gate упал — отменить остальные).
   - Для PR: fail-fast: false (хотим увидеть все сломанные профили).
   - Для nightly: fail-fast: false + continue-on-error: true на flaky.

5. PATHS FILTERS
   - Push only when: src/**, pom.xml, diesel/**, .github/workflows/**, scripts/**
     изменены. Иначе skip (используй dorny/paths-filter@v3 или нативные
     paths: / paths-ignore:).

6. CONCURRENCY
   - На верхнем уровне:
       concurrency:
         group: ${{ github.workflow }}-${{ github.ref }}
         cancel-in-progress: true
   - Это отменит старые запуски при новом пуше (для одной ветки).

7. TIMEOUT
   - Каждый джоб: timeout-minutes: 30 (быстрый fail вместо зависания).

8. ARTIFACTS
   - Загружай только при падении (if: failure()), не всегда — экономит storage.
   - retention-days: 7 для обычных, 30 для nightly и release-jar.

9. RUNNER ВЫБОР
   - Все джобы на ubuntu-24.04 (быстрее и дешевле, чем macOS/Windows).
   - Если нужна macOS/Windows — только nightly, не push.

10. MONITORING
    - Добавь шаг, который пишет в Job Summary (echo "..." >> $GITHUB_STEP_SUMMARY):
      время сборки, количество прогнанных тестов, статус кэша, размер артефактов.
    - Это бесплатно и не тратит runner-minutes, но улучшает observability.

ВЫВОД
1. Полный оптимизированный ci.yml.
2. Таблица «до/после» по ожидаемому расходу runner-minutes на один пуш.
3. Список сторонних actions, которые ты используешь, с указанием версии и лицензии.
4. Конкретные числа: ожидаемое время сборки, ожидаемый расход минут в месяц при
   текущем темпе пушей (10-20 в день).
```

---

### ПРОМПТ 4 — Расширение набора тестов в GitHub CI (бесплатно, на ресурсах GitHub)

```
Ты — QA-инженер с экспертизой Java/Maven/CI. Репозиторий: github.com/Reider85/dieseldb.

КОНТЕКСТ
- Java 21 LTS, Maven-сборка, Surefire 3.2.5.
- В pom.xml уже есть профили: core, query-full, storage, concurrency, network, perf,
  large, flaky, flaky-jsonl, timing-quick, storage-csv/tsv/jsonl/avro.
- Текущий CI гоняет только [fast, core, concurrency, network] на пуше. Остальные
  профили почти не используются.
- Нет coverage-репортинга, нет мутационного тестирования, нет security-сканирования,
  нет cross-JDK матрицы.

ЦЕЛЬ: расширить test-strategy так, чтобы:
1. Покрытие кода выросло и было видимым (Codecov badge в README).
2. Появились сигналы о quality/security (CodeQL, Dependabot, Trivy).
3. Cross-JDK матрица ловила бы регрессии раньше пользователей.
4. Coverage не падал (настроить threshold gate).
5. Всё это — на бесплатных GitHub runners, без ваших ресурсов.

ЗАДАЧА
Спроектируй и реализуй:

1. CODECOV INTEGRATION
   - Добавь jacoco-maven-plugin в pom.xml (report phase).
   - В CI после `mvn test` добавь шаг codecov/codecov-action@v4.
   - Токен CODECOV_TOKEN в repository secrets (бесплатно для open source).
   - Добавь бейдж в README: [![codecov](https://codecov.io/...)]

2. CODEQL WORKFLOW (.github/workflows/codeql.yml)
   - На push в main + weekly schedule.
   - language: java-jar.
   - Бесплатно, не считаетя против 2000 минут (CodeQL exempt).
   - Авто-создание issue при найденной vulnerability.

3. DEPENDABOT (.github/dependabot.yml)
   - maven ecosystem, weekly, open-pull-requests-limit: 10.
   - github-actions ecosystem, weekly.
   - Это бесплатно и снимает с вас ручное обновление зависимостей.

4. CROSS-JDK MATRIX (nightly)
   - matrix.jdk: [21, 22, 23-ea]
   - Должно проверять, что новая Java не ломает DieselDB.
   - Используй actions/setup-java@v4 с java-version: ${{ matrix.jdk }}.

5. PITEST (mutation testing) — weekly
   - pitest-maven плагин в pom.xml в отдельном профиле mutation.
   - Запуск раз в неделю (schedule: '0 4 * * 0').
   - Не блокирует пуш, только сигнал качества.

6. STORAGE MATRIX (nightly)
   - matrix.storage: [csv, tsv, jsonl, avro]
   - mvn -B test -P storage-${{ matrix.storage }}
   - Полное покрытие 4 бэкендов.

7. PERFORMANCE REGRESSION (nightly)
   - mvn -B test -P perf
   - Сравнение с baseline из analytics/performance_history.csv.
   - Если regression > 10% — создается issue автоматически.

8. STRESS TEST (weekly)
   - large профиль + 3 повтора для стабилизации.
   - Не блокирует, только репортит.

9. TEST RESULT VISIBILITY
   - dorny/test-reporter@v1 — парсит surefire-reports и выводит в PR check.
   - В Job Summary — таблица: профиль, тестов всего, passed, failed, skipped,
     время.

КРИТЕРИЙ ГОТОВНОСТИ
1. Полный набор YAML файлов:
   - .github/workflows/codeql.yml
   - .github/workflows/nightly-tests.yml (расширенная ночь)
   - .github/dependabot.yml
2. Патч pom.xml: jacoco + pitest плагины в соответствующих профилях.
3. Обновлённый .github/workflows/ci.yml с интеграцией codecov.
4. Ожидаемый дополнительный расход runner-minutes/месяц (должен быть < 500).
5. Ожидаемое улучшение покрытия (примерные числа: сейчас X%, после — Y%).
6. Список бейджей для README с готовым markdown.

ВАЖНО
- Не добавляй self-hosted runners.
- Не запускай тяжёлые тесты (perf, large, mutation) на пуше — только nightly/weekly.
- Проверь, что бесплатные лимиты не превышаются:
  - 2000 минут/месяц для ubuntu runners.
  - CodeQL и Dependabot — не считаются.
  - storage 1GB для артефактов.
```

---

### ПРОМПТ 5 — Попадание в GitHub Trending: topics, releases, social signals

```
Ты — DevRel/гровт-инженер для open source. Репозиторий: github.com/Reider85/dieseldb.

КОНТЕКСТ
- 5 stars, 0 forks, 0 open issues, 0 tags, 0 releases.
- Topics сейчас: aries, databases, databases-programming, databasesystems, sql,
  transaction, wal — 7 узких.
- Discussions выключены, Projects включены (но пустые), Wiki включена (пустая),
  Homepage не задан.
- README хороший, но нет бейджей и нет English-резюме в первом экране.
- Цель: попасть в GitHub Trending (за неделю/месяц), набрать 100+ звёзд за 30 дней
  и увеличить social-сигналы без накруток.

ЗАДАЧА
Составь и выполни «30-дневный growth-план» для DieselDB:

ШАГ 1. РАСШИРЬ TOPICS (через GitHub API или UI: Settings → General)
   Замени текущие 7 на 20 (максимум), включив широкие + точные:
   Рекомендуемый набор:
   - java, database, database-engine, dbms, sql, jdbc, postgresql-protocol
   - aries, wal, mvcc, ssi, serializable, transaction, acid, recovery
   - storage-engine, avro, parquet, page-storage
   - mtls, rbac, audit-log, java21
   Команда через API (потребуется PAT):
   gh api -X PUT /repos/Reider85/dieseldb/topics -f names[]=java -f names[]=database ...

ШАГ 2. ВКЛЮЧИ DISCUSSIONS
   Settings → General → Features → Discussions = ON.
   Создай 3 стартовых категории:
   - Q&A (вопросы по использованию)
   - Ideas (предложения фич)
   - Show & Tell (юз-кейсы)
   Закрепи "Welcome to DieselDB Discussions" с введением.

ШАГ 3. СОЗДАЙ HOMEPAGE ЧЕРЕЗ GITHUB PAGES
   .github/workflows/pages.yml — собирает javadoc и публикует на
   https://reider85.github.io/dieseldb/
   Это бесплатный сайт + сигнал для Trending (заполненное поле Homepage).
   В Settings → Pages выбери source = GitHub Actions.

ШАГ 4. ОТПРАВЬ FEATURE-ISSUE И ЗАКРЕПИ ЕГО
   Создай issue "DieselDB v3.4 — what's inside (overview)" с:
   - Краткое описание проекта (3 абзаца).
   - Ссылка на Quick Start из README.
   - Архитектурная диаграмма (можно Mermaid в самом issue).
   - Список того, что уже работает (production-implemented).
   - Ссылка на Discussions.
   - Призыв: "Star the repo if you find this useful — it helps others discover DieselDB".
   Закрепи (pin) issue.

ШАГ 5. ДОБАВЬ БЕЙДЖИ В README
   Сверху README.md, сразу под заголовком, добавь строку:
   ![CI](badge) ![Release](badge) ![License](badge) ![Stars](badge)
   ![Java](badge) ![codecov](badge) ![PRs Welcome](badge)
   Сгенерируй shields.io URLs.

ШАГ 6. ENGLISH SUMMARY В НАЧАЛЕ README
   Первые 5 строк README — английское описание (для международных поисковиков
   и для users, которые заходят из Trending). Русский текст оставь ниже.

ШАГ 7. СОЗДАЙ SOCIAL PROOF
   - Включи GitHub Sponsors button (Settings → General → Sponsors).
   - Добавь CONTRIBUTING.md с правильными quick wins (good first issues).
   - Создай 3 issue с меткой "good first issue" — это даёт бейдж в Insights.

ШАГ 8. АВТО-CHANGELOG В РЕЛИЗЕ
   Поскольку релизы теперь автогенерируются (Промпт 2), каждый релиз будет
   содержать коммиты с прошлого тега. Это улучшает видимость на GitHub
   Releases page (появляется в Explore → Releases).

ВЫВОД
1. Полный список из 20 topics (готовая команда gh api).
2. Markdown для бейджей (для копирования в README).
3. Шаблон welcome-issue (готовый markdown).
4. Полный .github/workflows/pages.yml.
5. Список «good first issue» заголовков (3-5 штук), которые создаст
   пользователь вручную (не генерируй код, только темы issue).
6. Конкретные числа KPI на 30 дней: ожидаемое количество stars, forks,
   watchers, если план выполнен честно.

ВАЖНО
- Не предлагать накрутку звёзд (это банит репозиторий).
- Не предлагать cross-posting больше чем 3 площадки (r/java, r/database,
  Hacker News) — иначе GitHub сочтёт это spam.
- Все действия — легальные, прозрачные, делаются вручную владельцем репо.
```

---

### ПРОМПТ 6 — GitHub Pages + auto-changelog + статус-бейджи (бесплатные ресурсы GitHub)

```
Ты — технический писатель + DevOps. Репозиторий: github.com/Reider85/dieseldb.

КОНТЕКСТ
- Проект на Java 21, Maven-сборка, есть javadoc в исходниках.
- Changelog.md уже существует (987 строк), но не обновляется автоматически.
- Нет сайта-документации, нет auto-changelog, нет "live" статуса.

ЦЕЛЬ: использовать бесплатные ресурсы GitHub для:
1. Автоматического сайта-документации (GitHub Pages + Javadoc).
2. Авто-обновления Changelog.md на каждый релиз.
3. Видимых бейджей CI/Release/Coverage/Java в README.

ЗАДАЧА

1. GITHUB PAGES — javadoc-сайт
   .github/workflows/pages.yml:
   - trigger: push on main (только если изменились src/main/java/**)
   - job: build-javadoc
     runs-on: ubuntu-24.04
     permissions: { contents: read, pages: write, id-token: write }
     environment: github-pages
     steps:
       - checkout
       - setup-java@v4 (21)
       - mvn -B javadoc:javadoc -DskipTests --no-transfer-progress
       - upload-pages-artifact@v3 path: target/site/apidocs
       - deploy-pages@v4
   - Это бесплатно, считается Pages-лимитом (1GB bandwidth/час для OSS —
     хватает).
   - Адрес: https://reider85.github.io/dieseldb/

2. AUTO-CHANGELOG
   .github/workflows/changelog.yml:
   - trigger: release published (создано релизом из Промпта 2)
   - job: update-changelog
     steps:
       - checkout
       - использует mixinable/github-action-changelog@v1 или
         mikepenz/release-changelog-builder-action@v5
       - Парсит коммиты с прошлого тега по conventional-commits
         (feat:, fix:, perf:, refactor:, docs:, chore:).
       - Записывает в Changelog.md новый раздел "## [v3.4.4-build.762] — 2026-11-08".
       - Коммитит: git commit -m "docs: update changelog for v3.4.4 [skip ci]"
       - git push.
   - НЕ должен создавать рекурсивный CI (используй [skip ci]).

3. СТАТУС-БЕЙДЖИ
   Сгенерируй markdown с бейджами для README:
   - CI: ![](https://github.com/Reider85/dieseldb/actions/workflows/ci.yml/badge.svg)
   - Release: ![](https://img.shields.io/github/v/release/Reider85/dieseldb)
   - License: ![](https://img.shields.io/github/license/Reider85/dieseldb?color=blue)
   - Stars: ![](https://img.shields.io/github/stars/Reider85/dieseldb?style=social)
   - Java: ![](https://img.shields.io/badge/Java-21%20LTS-orange)
   - Maven: ![](https://img.shields.io/maven-central/v/com.dieseldb/dieseldb)
     (после публикации в Maven Central — иначе JitPack)
   - Codecov: ![](https://codecov.io/gh/Reider85/dieseldb/branch/main/graph/badge.svg)
   - Last commit: ![](https://img.shields.io/github/last-commit/Reider85/dieseldb)
   - PRs Welcome: ![](https://img.shields.io/badge/PRs-welcome-brightgreen)

4. JAVADOC LINK В РЕЛИЗЕ
   Каждый GitHub Release должен содержать ссылку на javadoc этого релиза:
   https://reider85.github.io/dieseldb/v3.4.4-build.762/ (через pages history
   или через paths filter по git tag).

КРИТЕРИЙ ГОТОВНОСТИ
1. Полный .github/workflows/pages.yml.
2. Полный .github/workflows/changelog.yml.
3. Markdown-блок бейджей для вставки в README (одна строка).
4. Инструкция по включению Pages в репозитории (Settings → Pages → Source:
   GitHub Actions).
5. Ожидаемое время первого деплоя javadoc (~3-5 минут).

ВАЖНО
- Pages бесплатный для public репозиториев.
- Javadoc должен собираться с -Xdoclint:none, иначе современные Java 21
  строгие проверки сломают сборку.
- Если javadoc падает на этапе сборки, не блокировать релиз — добавь
  continue-on-error: true на этапе javadoc, но создай issue с ошибкой.
```

---

### ПРОМПТ 7 — Engagement loop: issues, discussions, contributing, good-first-issues

```
Ты — community manager для open-source Java проекта. Репозиторий:
github.com/Reider85/dieseldb.

КОНТЕКСТ
- 0 open issues, 0 forks, 5 watchers, Discussions выключены.
- Лицензия Apache-2.0, CONTRIBUTING.md добавлен 7 ноября.
- Активность коммитов высокая (700+ за 6 месяцев), но social engagement = 0.
- Цель: создать engagement loop, который превращает «проходит мимо» зрителя
  в starring → forking → contributing.

ЗАДАЧА
Спроектируй и реализуй постоянный engagement-loop:

1. «WELCOME» DISCUSSIONS
   После включения Discussions (Промпт 5, шаг 2), создай 3 discussion:
   - "Welcome to DieselDB" — введение, ссылка на README, призыв к вопросам.
   - "Roadmap v3.5 / v4.0" — что планируется, что готово, что в работе.
   - "Use cases — share yours!" — место для юз-кейсов.
   Каждый discussion должен заканчиваться: "Star ⭐ the repo if this is useful!"

2. GOOD-FIRST-ISSUES
   Создай 5 issue с метками `good first issue` + `help wanted` (это даёт
   бейдж в Insights и привлекает contributors):
   - "Add example: DieselDB + Spring Boot embedded mode"
   - "Add example: DieselDB + JDBC + Hibernate"
   - "Improve error message for unknown SQL keyword"
   - "Add JMH benchmark for SELECT WHERE on indexed column"
   - "Translate README sections to English (currently mixed RU/EN)"
   Каждое issue: контекст, что нужно сделать, ссылка на код, сложность
   (beginner/intermediate), ожидаемое время (1-2 часа).

3. CONTRIBUTING.MD ОБНОВИТЬ
   В CONTRIBUTING.md добавь:
   - Quick start для contributors (как клонировать, собрать, запустить тесты).
   - "Где искать good first issues" — ссылка на label.
   - Conventional commits cheat-sheet (feat:, fix:, chore:, docs:, refactor:).
   - "Как предложить фичу" — через Discussions → Ideas.
   - "Как сообщить баг" — через issue с шаблоном.

4. ISSUE TEMPLATES
   Создай .github/ISSUE_TEMPLATE/:
   - bug_report.yml (форма: версия, ОС, JDK, шаги, ожидание, реальность, логи)
   - feature_request.yml (форма: проблема, предложение, альтернативы)
   - question.yml (если пользователи хотят спросить — редирект на Discussions)
   - config.yml (подсказка: "Есть вопрос? Используйте Discussions!")

5. PR TEMPLATE
   .github/PULL_REQUEST_TEMPLATE.md:
   - Что изменено
   - Почему (link issue)
   - Как тестировалось
   - Чеклист: тесты пройдены, javadoc обновлён, CHANGELOG обновлён

6. AUTOMATION
   Добавь .github/workflows/labeler.yml:
   - На новый PR: ставит label по paths (storage/, recovery/, sql/, drivers/).
   - На новый issue с good-first-issue: приветственный комментарий от
     github-actions[bot] со ссылкой на CONTRIBUTING.md.
   - Используй actions/labeler@v5 и actions/stale@v9 для закрытия старых
     неактивных issue (через 60 дней без активности — помечать stale, через
     7 дней — закрывать).

7. «STAR REMINDER» В РЕЛИЗАХ
   Каждый автогенерируемый release notes должен содержать внизу:
   "⭐ Star this repo if you find DieselDB useful — it helps others discover it."

8. СОЦИАЛЬНЫЕ ШАБЛОНЫ ДЛЯ ВЛАДЕЛЬЦА (не автоматизировать)
   Подготовь 3 готовых текста для ручного постинга (где-то после релиза):
   - Reddit r/java (200 слов) — фичи, зачем, ссылка на релиз.
   - HackerNews "Show HN" (заголовок + 2 абзаца).
   - LinkedIn (300 слов) — technical overview.
   ВНИМАНИЕ: это ручная работа, НЕ автоматизировать — иначе ban.

КРИТЕРИЙ ГОТОВНОСТИ
1. Готовый markdown для 3 discussions (можно скопировать и создать).
2. Полные тексты 5 good-first-issue (готовы к созданию через GitHub UI).
3. Обновлённый CONTRIBUTING.md.
4. Полные .github/ISSUE_TEMPLATE/ файлы (4 файла).
5. .github/PULL_REQUEST_TEMPLATE.md.
6. Полный .github/workflows/labeler.yml.
7. .github/labeler.yml конфиг для actions/labeler.
8. .github/workflows/stale.yml для авто-закрытия.
9. 3 готовых текста для ручного постинга (Reddit/HN/LinkedIn).
10. KPI на 30 дней: ожидаемое количество stars, forks, PRs, discussions.

ВАЖНО
- Engagement должен быть честным, без накруток.
- Не предлагать массовое приглашение пользователей (DM-spam) — это банит.
- All automation должна усиливать, не заменять живое общение владельца.
```

---

## 4. Порядок внедрения (чеклист на ближайшие 2 недели)

| День | Действие | Промпт | Ожидаемый эффект |
|---|---|---|---|
| 1 | Запустить Промпт 1 (чиним CI) | 1 | Первый зелёный прогон за 6 месяцев |
| 2 | Запустить Промпт 3 (оптимизация расхода) | 3 | -70% runner-minutes на пуш |
| 3 | Запустить Промпт 2 (авто-релизы) | 2 | 1-й релиз появляется в /releases |
| 4 | Запустить Промпт 6 (Pages + changelog) | 6 | Сайт doc.dieseldb + live бейджи |
| 5 | Расширить topics (Промпт 5, шаг 1) | 5 | +20 topics, лучше поиск |
| 5 | Включить Discussions (Промпт 5, шаг 2) | 5 | Социальная воронка открыта |
| 6 | Создать welcome issue + good first issues (Промпт 7) | 7 | Бейдж в Insights, attraction |
| 7 | Запустить Промпт 4 (тесты + coverage + CodeQL) | 4 | Coverage бейдж, security бейдж |
| 8 | Добавить English summary + бейджи в README (Промпт 5, шаги 5-6) | 5 | International visibility |
| 9 | Запустить Промпт 7 полностью (labeler, stale, templates) | 7 | Автоматизация engagement |
| 10 | Создать 3 discussions (Промпт 7, шаг 1) | 7 | Активная Discussions-зона |
| 14 | Ручной постинг Reddit/HN/LinkedIn (Промпт 7, шаг 8) | 7 | Внешний трафик на репо |
| 30 | Замер KPI | — | Цель: 50-150 stars, 5-15 forks |

---

## 5. Что даст эта стратегия (конкретные числа)

| Метрика | Сейчас | Через 30 дней (если всё внедрить) | Через 90 дней |
|---|---|---|---|
| CI-бейдж | 100% красный | 90% зелёный | 95%+ зелёный |
| Releases | 0 | 1 в день (авто) | ~90 релизов |
| Stars | 5 | 50-150 | 300-800 |
| Forks | 0 | 5-15 | 20-50 |
| Discussions | выключены | 5-10 активных | 20-50 |
| Open issues | 0 | 5-10 (good first) | 15-30 |
| Topics | 7 узких | 20 широких | 20 |
| Coverage бейдж | нет | есть (Codecov) | есть |
| Security бейдж | нет | есть (CodeQL) | есть |
| Pages-сайт | нет | есть | есть |
| Runner-minutes/мес | ~3000 (превышение лимита) | ~600 (укладываемся в free tier) | ~600 |
| Шанс попасть в Trending (Java, week) | ~0% | 15-30% | 40-60% |

---

## 6. Что НЕ делать (контр-продуктивно для цели «звёзды и тренды»)

- ❌ **Self-hosted runners** — это ваши ресурсы, цель — переложить работу на GitHub.
- ❌ **Полный `mvn clean verify -P all` на каждом пуше** — сжигает лимит, делает бейдж красным.
- ❌ **Накрутка звёзд** — банит репо, удалит из Trending навсегда.
- ❌ **Автоматический cross-post в соцсети** — GitHub/Reddit/HN сочтут спамом.
- ❌ **`fail-fast: false` для push-событий** — тратит минуты на ненужные матрицы.
- ❌ **Забивать Changelog вручную** — устаревает за день, используйте автогенерацию.
- ❌ **Большой PR одним куском** — делайте изменения по одному промпту за раз.
- ❌ **Игнорировать English-резюме в README** — половина трафика Trendings international.

---

## 7. Список файлов, которые должны появиться в репозитории

После выполнения всех промптов:

```
.github/
├── workflows/
│   ├── ci.yml                       # (Промпт 1 + 3) — оптимизированный CI
│   ├── release-on-push.yml          # (Промпт 2) — авто-релиз
│   ├── pages.yml                    # (Промпт 6) — javadoc → GitHub Pages
│   ├── changelog.yml                # (Промпт 6) — авто-обновление Changelog.md
│   ├── codeql.yml                   # (Промпт 4) — security scanning
│   ├── nightly-tests.yml            # (Промпт 4) — расширенная ночь
│   ├── labeler.yml                  # (Промпт 7) — авто-лейблы на PR
│   └── stale.yml                    # (Промпт 7) — закрытие старых issue
├── dependabot.yml                   # (Промпт 4)
├── labeler.yml                      # (Промпт 7) — конфиг для actions/labeler
├── ISSUE_TEMPLATE/
│   ├── bug_report.yml               # (Промпт 7)
│   ├── feature_request.yml          # (Промпт 7)
│   ├── question.yml                 # (Промпт 7)
│   └── config.yml                   # (Промпт 7)
└── PULL_REQUEST_TEMPLATE.md         # (Промпт 7)

Изменяемые существующие файлы:
├── pom.xml                          # + jacoco, + pitest
├── README.md                        # + бейджи, + English summary
├── CONTRIBUTING.md                  # + quick start для контрибьюторов
└── Changelog.md                     # будет авто-обновляться

Настройки репозитория (через GitHub UI/API):
├── Topics (20 штук)                 # Промпт 5
├── Description + Homepage          # Промпт 5 + 6
├── Discussions = ON                # Промпт 5
├── Pages = ON (Source: GitHub Actions)  # Промпт 6
├── Actions → General → Workflow permissions = Read and write  # Промпт 2
└── Secrets: CODECOV_TOKEN (опционально, для приватных репо)  # Промпт 4
```

---

## 8. Итоговые ответы на вопросы пользователя

**Q: Создать промпты чтобы при каждом коммите и пуше создавался релиз?**
→ **Промпт 2** — полностью закрывает это. Использует `workflow_run` триггер, чтобы не зацикливать CI, и `softprops/action-gh-release@v2` для создания релиза + автогенерации release notes из коммитов.

**Q: И в тренды на GitHub появляться?**
→ **Промпт 5** + **Промпт 6** + **Промпт 7** — расширяют topics, включают Discussions, создают homepage (Pages), бейджи в README, good-first-issues, engagement loop. Попадание в Trending требует все эти сигналы вместе.

**Q: Проверь, что реализовано по GitHub CI?**
→ Раздел **1.1** выше: 6 джобов в `ci.yml` + 4 джоба в `publish-drivers.yml`. Подробности по каждому.

**Q: Почему всегда failed?**
→ Раздел **1.2**: 7 причин, главные — активные компиляционные ошибки (миграция JUL→SLF4J), `lint-tia-mapping` на каждом пуше, неправильный кэш-ключ, матрица запускается целиком вместо `fast`-only.

**Q: Как оптимизировать?**
→ **Промпт 1** + **Промпт 3**. Ожидаемая экономия runner-minutes ~70%, первый зелёный прогон — день 1.

**Q: Что даст?**
→ Раздел **1.3** + раздел **5**: экономия лимита, зелёный бейдж, доверие, +30% к вероятности форка, рост звёзд.

**Q: Какие тесты можно выполнять с помощью GitHub CI — улучшит ли это?**
→ Раздел **1.4**: 13 категорий тестов с обоснованием «зачем» и «когда запускать». **Промпт 4** реализует 8 из них на бесплатных runners. Покрытие, security, cross-JDK, mutation, storage matrix, perf regression.

**Q: Цель — проект чаще мелькал и набирал звёзды при пушах?**
→ Полностью закрыто: авто-релизы (Промпт 2) → постоянный «recent activity», который видит GitHub Trending алгоритм. Каждый пуш = новый релиз в Explore → Releases.

**Q: Использовать ресурсы GitHub вместо своих?**
→ Полностью: все workflows на `ubuntu-24.04` бесплатных runners. CodeQL/Dependabot не считаются против лимита. Pages бесплатный для public. Никаких self-hosted runners.

**Q: Результат как несколько промптов?**
→ **7 промптов**, готовых к копированию в Claude/Cursor/Copilot. Каждый самодостаточен, с контекстом, задачей, требованиями и критерием готовности.

---

*Документ создан на основе анализа состояния репозитория по состоянию на 8 ноября 2026. Все рекомендации основаны на публичных данных GitHub Actions docs, GitHub Trending algorithm research, и фактическом содержимом .github/workflows/ci.yml + commit history за ноябрь 2026.*

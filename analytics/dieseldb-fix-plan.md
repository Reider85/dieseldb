# План исправлений DieselDB после аудита миграции тест-профилей

> **Для кого этот документ:** ИИ-агент (или инженер), исполняющий исправления пошагово.
> **Формат:** исполняемый Markdown TODO. Каждый шаг содержит: цель, действия, дословный код/команды, команды валидации, типичные ошибки, rollback.
> **Базовый репозиторий:** `https://github.com/Reider85/dieseldb` (main @ `4eb3f9a`, версия 3.1.12)
> **Источник аудита:** проверка плана `dieseldb-test-migration-plan.md` от 2026-09-17. Шаги 2–5, 7 исходной миграции выполнены; шаги 6, 8, 10 — с отклонениями; шаг 9 (Maven Build Cache) — не выполнен.
> **Цель:** довести профильную систему до заявленных в миграции гарантий — 100% покрытие тестов профилями, рабочий acceptance-gate, TIA в CI и локально, Maven Build Cache.

---

## 0. Пререквизиты и конвенции

### 0.1. Рабочая среда

- Репозиторий склонирован, все действия из корня: `cd /home/z/my-project/dieseldb` (или локальная копия на Windows-машине разработки)
- Maven 3.9+, JDK 21. На Windows — префикс из AGENTS.md: `$env:JAVA_HOME = "..."; & "C:\tools\apache-maven-3.9.9\bin\mvn.cmd" <args>`
- Для шага 4 (tia.sh) нужен `bash` (Git Bash на Windows, есть на ubuntu-runner CI)
- Для шага 2 нужен `python3` (на Windows — `python`)

### 0.2. Глобальные правила

1. **Каждый шаг — отдельный git-коммит.** Название: `fix(N): <шаг> — <описание>` (номера соответствуют шагам этого плана).
2. **Перед шагом** — чистый `git status`; **после шага** — валидация. Валидация упала → откат (`git checkout -- .` или `git revert HEAD`), не переходить к следующему шагу.
3. **Не трогать** исходники `diesel/` — весь план касается только тестовой инфраструктуры, `scripts/`, `.github/`, `Makefile`, `pom.xml`, `AGENTS.md`.
4. Никаких эмодзи в коде/XML/YAML (в существующем `compare-timing.sh` эмодзи уже есть — не трогать, его не переписываем).

### 0.3. Ветка

```bash
git checkout main
git pull origin main
git status                                    # должно быть clean
git checkout -b fix/test-profiles-audit
```

### 0.4. Сводка шагов и приоритеты

| Шаг | Приоритет | Проблема (из аудита) | Файлы |
|---|---|---|---|
| 1 | **P0** | 3 тест-класса без `@Tag` — не запускаются ни в одном профиле (тесты-сироты) | `src/test/java/diesel/CsvStorageTest.java`, `TsvStorageTest.java`, `RegexRobustnessTest.java`, `scripts/tag-mapping.tsv` |
| 2 | **P0** | `make timing` молча запускает 0 тестов; baseline `timing/timing.md` не в git; `timingN.md` никто не генерирует | `Makefile`, `.gitignore`, `scripts/collect-timing.py` (новый), `timing/timing.md` |
| 3 | P1 | Alias-профили `test`/`ci` неработоспособны (свойство `surefire.groups` никто не читает) | `pom.xml` |
| 4 | P1 | TIA только на PowerShell; выводит нерабочую команду `mvn -Dgroups=...` без профиля; маппинг неполный | `scripts/tia.sh` (новый), `scripts/tia.ps1`, `scripts/tia-mapping.txt`, `Makefile` |
| 5 | P1 | Job `pr-gate-tia` отсутствует в CI (п. 8.4 исходного плана не выполнен) | `.github/workflows/ci.yml` |
| 6 | P2 | Шаг 9 исходной миграции не выполнен: нет Maven Build Cache | `.mvn/` (новое), `pom.xml`, `.gitignore` |
| 7 | P2 | `make build` сломан; concurrency-group CI ссылается на undefined `matrix.profile` | `Makefile`, `.github/workflows/ci.yml` |
| 8 | P2 | AGENTS.md устарел: `make quick-test`, `-DskipLargeTests`, старое описание acceptance gate | `AGENTS.md` |
| 9 | — | Финальная валидация, пуш, PR | — |

### 0.5. Ключевой факт, на котором держатся шаги 1, 3, 4, 5

Дефолтная конфигурация surefire в `pom.xml` содержит `<excludes>**/*.java</excludes>` — без профиля Maven запускает 0 тестов. Следствия:
- `mvn test -Dgroups="..."` **без `-P` профиля не работает** (excludes глушат всё) — исходный план и `tia.ps1` это не учитывали;
- `-Dtest=Тест#метод` без профиля тоже запускает 0 тестов — изолированные прогоны (см. AGENTS.md) обязаны указывать профиль;
- теги JUnit 5 (`@Tag`) — единственный механизм выбора тестов внутри профиля, поэтому класс без тега недостижим ни в одном профиле.

---

## Шаг 1. Теги для трёх тестов-сирот (P0)

### Цель
После миграции 90 из 98 тест-файлов получили `@Tag`. Три класса из карты плана остались без тега и теперь **не запускаются ни в одном профиле**:

| Класс | Должен быть | Причина пропуска |
|---|---|---|
| `CsvStorageTest` | `storage` | класс package-private (`class CsvStorageTest`), скрипт тегирования искал только `^public class` |
| `TsvStorageTest` | `storage` | та же причина; кроме того, их нет в `scripts/tag-mapping.tsv` |
| `RegexRobustnessTest` | `perf` | файл minified (весь код в одной строке), паттерн `^public class` не сматчил; от скрипта остался неиспользуемый `import org.junit.jupiter.api.Tag;` |

### Действия

```bash
cd /home/z/my-project/dieseldb
git status    # чисто

# 1.1. CsvStorageTest: импорт + аннотация (класс package-private!)
sed -i 's|^import java.time.LocalDateTime;|import org.junit.jupiter.api.Tag;\nimport java.time.LocalDateTime;|' src/test/java/diesel/CsvStorageTest.java
sed -i 's|^class CsvStorageTest {|@Tag("storage")\nclass CsvStorageTest {|' src/test/java/diesel/CsvStorageTest.java

# 1.2. TsvStorageTest: то же самое
sed -i 's|^import java.time.LocalDateTime;|import org.junit.jupiter.api.Tag;\nimport java.time.LocalDateTime;|' src/test/java/diesel/TsvStorageTest.java
sed -i 's|^class TsvStorageTest {|@Tag("storage")\nclass TsvStorageTest {|' src/test/java/diesel/TsvStorageTest.java

# 1.3. RegexRobustnessTest: файл minified — одна строка, работаем по подстроке (БЕЗ ^).
# Импорт org.junit.jupiter.api.Tag уже есть (добавлен скриптом, но аннотацию он не вставил).
sed -i 's|public class RegexRobustnessTest {|@Tag("perf") public class RegexRobustnessTest {|' src/test/java/diesel/RegexRobustnessTest.java
```

Внимание к `RegexRobustnessTest`: у файла всего 2 физические строки, `public class RegexRobustnessTest {` находится **внутри** строки 2. Якорь без `^` обязателен. После правки импорт `Tag` станет используемым — лишний импорт исчезает сам.

Добавить недостающие строки в `scripts/tag-mapping.tsv` (для воспроизводимости, после строки с `TsvStorageAdvancedTest`):

```
CsvStorageTest  storage
TsvStorageTest  storage
```

(разделитель — таб)

### Валидация

```bash
# 1. Все три класса тегированы
grep -l '@Tag("storage")' src/test/java/diesel/CsvStorageTest.java src/test/java/diesel/TsvStorageTest.java
grep -l '@Tag("perf")'    src/test/java/diesel/RegexRobustnessTest.java

# 2. Общий счётчик: было 90, должно стать 93
grep -lE '@Tag\(' src/test/java/diesel/*.java | wc -l     # ожидаемо: 93

# 3. Нет дублей тега в одном файле
for f in src/test/java/diesel/*.java; do
  n=$(grep -o '@Tag(' "$f" | wc -l)
  [ "$n" -le 1 ] || echo "DUP: $f ($n)"
done    # вывод должен быть пустым (класс LargeTest.java с @Tag("large") — это аннотация, не тест; допускается 1)

# 4. Компиляция
mvn -B test-compile -q && echo "COMPILE OK"

# 5. Тесты реально попадают в профили
mvn -B clean test -P core 2>&1 | grep -E "CsvStorageTest|TsvStorageTest"
# ожидаемо: строки "CsvStorageTest Tests run: ..." и "TsvStorageTest Tests run: ..."
mvn -B clean test -P perf 2>&1 | grep -E "RegexRobustnessTest"
# ожидаемо: "RegexRobustnessTest Tests run: 17 ..." (все @Test методы класса)
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| Дубль `@Tag` в файле | Шаг запущен повторно | `git checkout -- <файл>` и начать шаг заново |
| `sed` не сработал на RegexRobustnessTest | Использован якорь `^public class` | Якорь без `^` — класс в середине строки |
| Тест по-прежнему не запускается в профиле | Тег есть, но класс упомянут в старых `<excludes>` | Проверить: `mvn -B help:effective-pom -P core | grep -A3 excludes` — переопределение `<excludes combine.self="override"/>` уже стоит в профиле core |

### Rollback

```bash
git checkout -- src/test/java/diesel/ scripts/tag-mapping.tsv
```

### Коммит

```bash
git add src/test/java/diesel/CsvStorageTest.java src/test/java/diesel/TsvStorageTest.java src/test/java/diesel/RegexRobustnessTest.java scripts/tag-mapping.tsv
git commit -m "fix(1): tag 3 orphaned test classes (CsvStorageTest, TsvStorageTest -> storage, RegexRobustnessTest -> perf)

These classes were missed by the migration tagging script (package-private
class declarations and a minified single-line file) and were silently
skipped by every profile. Test coverage via profiles is now complete:
93 tagged files, helpers intentionally untagged."
```

---

## Шаг 2. Починка acceptance gate: `make timing` (P0)

### Цель
Сейчас таргет `timing` выполняет `mvn -Ddiesel.largeTests=true -Dtest.heap=4g test` — **без профиля, т.е. 0 тестов**. Кроме того, выявлено два доломанных факта:
- `timing/timing.md` (baseline) **не закоммичен**: `.gitignore` содержит `timing/`, `git ls-files timing/` пуст — «tracked baseline, never delete it» из AGENTS.md не соответствует действительности;
- файл `timing/timingN.md` **никто не генерирует** ни в коде, ни в скриптах — сравнение всегда падало в ветку «Warning: timing/timingN.md not found».

Делаем gate детерминированным: прогон `large`-профиля (600x600 ORDER BY joins из `QuantitativeTest` — это и есть timing-нагрузка) → сбор пер-тестовых времён из surefire-отчётов в `timing/timingN.md` → сравнение с baseline через существующий `compare-timing.sh` (формат: строки `<test_name> <секунды>`, заголовок со словом `Test` игнорируется).

### Действия

#### 2.1. Скрипт сбора таймингов — новый файл `scripts/collect-timing.py`

```python
#!/usr/bin/env python3
# collect-timing.py - collect per-test wall times from surefire XML reports
# Output: timing/timingN.md in the format consumed by compare-timing.sh:
#   <test_name> <seconds>
# Usage: python3 scripts/collect-timing.py   (run from repo root after mvn test)

import glob
import os
import xml.etree.ElementTree as ET

ROWS = []
for path in glob.glob("target/surefire-reports/TEST-*.xml"):
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as e:
        print("WARN: cannot parse %s: %s" % (path, e))
        continue
    for tc in root.iter("testcase"):
        cls = (tc.get("classname") or "").split(".")[-1]
        name = "%s.%s" % (cls, tc.get("name") or "")
        try:
            secs = float(tc.get("time") or 0.0)
        except ValueError:
            secs = 0.0
        ROWS.append((name, secs))

ROWS.sort(key=lambda r: -r[1])

os.makedirs("timing", exist_ok=True)
with open("timing/timingN.md", "w", encoding="utf-8") as out:
    out.write("Test Time(s)\n")
    for name, secs in ROWS:
        out.write("%s %.3f\n" % (name, secs))

print("Wrote %d rows to timing/timingN.md" % len(ROWS))
```

#### 2.2. `.gitignore`: заменить строку `timing/` на

```
timing/*
!timing/timing.md
```

(именно `timing/*`, а не `timing/` — иначе негативный паттерн `!timing/timing.md` не сработает: gitignore не «заходит» в полностью игнорируемую директорию).

#### 2.3. `Makefile`: заменить таргет `timing` и добавить переменную `PY`

В начало Makefile (после `MVN_PATH ?= ...`) добавить:

```makefile
PY ?= python3
```

Существующий таргет `timing` заменить целиком на:

```makefile
## Full acceptance gate: large profile (4GB heap) + timing compare vs baseline
timing:
        @echo "Running acceptance gate: large profile (600x600 joins, 4GB heap)..."
        $(MVN) -B clean test -P large
        $(PY) scripts/collect-timing.py
        @if [ -f timing/timing.md ]; then \
                ./compare-timing.sh timing/timing.md timing/timingN.md; \
        else \
                echo "Baseline timing/timing.md not found - creating it from this run."; \
                cp timing/timingN.md timing/timing.md; \
                echo "Baseline created. Re-run 'make timing' to compare against it."; \
        fi
```

Обновить строку `.PHONY` — добавить `timing` уже есть, проверить, что ничего не удалилось.

#### 2.4. Закоммитить baseline

```bash
# На машине разработки, где baseline исторически существовал:
ls -la timing/timing.md && git add -f timing/timing.md
# Если файла нет локально - он будет создан первым прогоном make timing
# (см. валидацию ниже), тогда закоммитить после него.
```

### Валидация

```bash
# 1. Дефолтный прогон создаёт baseline (если его не было)
make timing 2>&1 | tail -15
# ожидаемо: BUILD SUCCESS, "Wrote N rows", "Baseline ... creating it"
# ВАЖНО: N > 0. Если N == 0 - large-профиль не запустил тесты, стоп и разбор.

# 2. Второй прогон сравнивает и проходит
make timing 2>&1 | tail -8
# ожидаемо: "Comparing ...", "PASSED: No significant regressions", exit 0

# 3. Baseline трекается
git ls-files timing/        # ожидаемо: timing/timing.md
git status -s               # timingN.md и прочие файлы timing/ НЕ видны

# 4. Сломанный сценарий отлавливается (санити-порога)
# (опционально) временно поднять время одного теста и убедиться, что
# compare-timing.sh возвращает exit 1; откатить.

# 5. Полный release-прогон не сломан (он отдельный таргет, не timing)
make all-tests 2>&1 | grep -cE "BUILD SUCCESS"   # ожидаемо: 6 (fast..large)
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `Wrote 0 rows` | Отчёты в `target/surefire-reports/` отсутствуют — прогон упал или профиль не тот | Проверить `ls target/surefire-reports/TEST-*.xml`; убедиться, что команда `-P large`, а не без профиля |
| `python3: command not found` (Windows) | Название интерпретатора | Запускать `make timing PY=python` |
| baseline не добавляется | `.gitignore` правлен как `timing/` | Только `timing/*` + `!timing/timing.md` (см. 2.2) |
| Сравнение с историческим baseline даёт сотни `NEW TEST` | Старый baseline в другом формате/с другими именами | Удалить старый файл и создать baseline заново первым прогоном — имена тестов изменились после миграции |
| `-P all` вместо `-P large` даёт OOM на join'ах | В профиле `all` жёсткий `-Xmx1g` | Не запускать heavy-join'ы через `all`; для полного прогона есть `make all-tests`, для heavy — `timing` |

### Rollback

```bash
git checkout -- Makefile .gitignore
git rm --cached timing/timing.md 2>/dev/null
rm -f scripts/collect-timing.py
```

### Коммит

```bash
git add Makefile .gitignore scripts/collect-timing.py
git add -f timing/timing.md 2>/dev/null || true
git commit -m "fix(2): repair make timing acceptance gate

- 'make timing' now runs the large profile (600x600 joins, 4GB heap) -
  the old command ran 'mvn test' without a profile, which executes 0 tests
  after the profile migration
- new scripts/collect-timing.py converts surefire XML reports into
  timing/timingN.md (format expected by compare-timing.sh); previously
  nothing generated that file, so the comparison step always warned
- baseline timing/timing.md is now tracked in git (.gitignore uses
  timing/* + !timing/timing.md); first run creates it if missing"
```

## Шаг 3. Рабочие alias-профили `test` / `ci` (P1)

### Цель
Alias-профили после миграции содержат только `<properties><surefire.groups>...` — но корневой конфиг surefire нигде не читает `${surefire.groups}` (там жёсткие `<excludes>**/*.java</excludes>`). Итог: `mvn clean test -P test` и `-P ci` молча запускают **0 тестов**, хотя обещана обратная совместимость. Дефект унаследован из самого исходного плана (его шаблон alias'а был таким же).

### Действия

В `pom.xml` заменить оба alias-профиля (в конце `<profiles>`) на полноценные:

**Было:**
```xml
<!-- Backward-compatible aliases for old profiles -->
<profile>
    <id>test</id>
    <properties>
        <surefire.groups>smoke, query, index</surefire.groups>
    </properties>
</profile>

<profile>
    <id>ci</id>
    <properties>
        <surefire.groups>smoke, query, index, query-full, storage, concurrency, network</surefire.groups>
    </properties>
</profile>
```

**Стало:**
```xml
<!-- Backward-compatible aliases for old profiles -->
<profile>
    <id>test</id>
    <properties>
        <test.heap>512m</test.heap>
        <surefire.groups>smoke, query, index</surefire.groups>
        <surefire.excludedGroups>large</surefire.excludedGroups>
    </properties>
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <excludes combine.self="override"/>
                    <groups>${surefire.groups}</groups>
                    <excludedGroups>${surefire.excludedGroups}</excludedGroups>
                    <forkCount>1</forkCount>
                    <reuseForks>true</reuseForks>
                    <parallel>classes</parallel>
                    <threadCountClasses>4</threadCountClasses>
                    <argLine>-Xmx512m --add-modules=jdk.incubator.vector</argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>
</profile>

<profile>
    <id>ci</id>
    <properties>
        <test.heap>1g</test.heap>
        <surefire.groups>smoke, query, index, query-full, storage, concurrency, network</surefire.groups>
        <surefire.excludedGroups>large</surefire.excludedGroups>
    </properties>
    <build>
        <plugins>
            <plugin>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <excludes combine.self="override"/>
                    <groups>${surefire.groups}</groups>
                    <excludedGroups>${surefire.excludedGroups}</excludedGroups>
                    <forkCount>2</forkCount>
                    <reuseForks>false</reuseForks>
                    <parallel>classes</parallel>
                    <threadCountClasses>2</threadCountClasses>
                    <argLine>-Xmx1g --add-modules=jdk.incubator.vector</argLine>
                    <systemPropertyVariables>
                        <diesel.largeTests>${diesel.largeTests}</diesel.largeTests>
                        <diesel.runAllTestsSample>true</diesel.runAllTestsSample>
                    </systemPropertyVariables>
                </configuration>
            </plugin>
        </plugins>
    </build>
</profile>
```

Пояснения: `test` теперь эквивалентен `fast`; `ci` повторяет старый охват (все теги, кроме perf/large) и включает `diesel.runAllTestsSample`, чтобы `AllTestsSampleTest` (`@Tag("network")` + гейтинг по свойству) запускался, как раньше.

### Валидация

```bash
mvn -B help:effective-pom -q > /dev/null && echo "POM OK"

# Alias запускает тесты (раньше - 0)
mvn -B clean test -P test 2>&1 | grep -E "Tests run:" | tail -1
# ожидаемо: Tests run: ~50-60 (тот же набор, что у fast)

mvn -B clean test -P ci 2>&1 | grep -E "Tests run:" | tail -1
# ожидаемо: Tests run: 300+ (fast + core + concurrency + network), AllTestsSampleTest включён

# Новые профили не сломаны
mvn -B clean test -P fast 2>&1 | grep -E "Tests run:" | tail -1
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `-P test` по-прежнему 0 тестов | Заменён только `<properties>`, без `<build>`-блока | Проверить, что в профиле есть `<configuration>` с `<groups>${surefire.groups}</groups>` и `<excludes combine.self="override"/>` |
| В `-P ci` отключён AllTestsSampleTest | Забыли `diesel.runAllTestsSample=true` | См. блок `<systemPropertyVariables>` выше |
| Дубль идентификатора профиля | Старый alias не удалён | В `pom.xml` должен остаться один профиль с `id=test` и один с `id=ci` |

### Rollback

```bash
git checkout -- pom.xml
```

### Коммит

```bash
git add pom.xml
git commit -m "fix(3): make legacy alias profiles test/ci actually run tests

The aliases only redefined the 'surefire.groups' property that no plugin
config consumes, so 'mvn test -P test|ci' silently ran 0 tests. They now
carry full surefire configuration (test == fast, ci == legacy coverage
without perf/large) and enable diesel.runAllTestsSample for the network
bucket."
```

---

## Шаг 4. TIA: bash-порт, рабочие команды, полный маппинг (P1)

### Цель
Три проблемы:
1. TIA существует только на PowerShell (`scripts/tia.ps1`) — исходный план требовал `tia.sh`; ubuntu-runner CI (шаг 5) и Linux-разработчики скрипт запустить не могут.
2. `tia.ps1` печатает команду `mvn -B clean test -Dgroups="..."` — она **не работает** без `-P` (дефолтные excludes глушат все тесты, см. §0.5).
3. `scripts/tia-mapping.txt` не покрывает `diesel/CliRepl.java` и классы исключений — изменения в них дают «нет тестов».

### Действия

#### 4.1. Новый файл `scripts/tia.sh` (bash-порт + режим `--profiles`)

```bash
#!/usr/bin/env bash
# tia.sh - Test-Impact Analysis for DieselDB (bash port of tia.ps1).
# Maps changed sources (git diff) to JUnit 5 @Tag buckets and Maven profiles.
#
# Usage:
#   ./scripts/tia.sh                    # show recommendations vs origin/main...HEAD
#   ./scripts/tia.sh HEAD~1             # compare with previous commit
#   ./scripts/tia.sh main..feature      # compare two refs
#   ./scripts/tia.sh --tags             # print tags only (comma-separated)
#   ./scripts/tia.sh --profiles         # print profile names only (space-separated)
#   ./scripts/tia.sh --run [ref]        # run recommended profiles sequentially

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

MAPPING_FILE="$REPO_ROOT/scripts/tia-mapping.txt"
if [[ ! -f "$MAPPING_FILE" ]]; then
    echo "Error: mapping file not found: $MAPPING_FILE" >&2
    exit 1
fi

REF="origin/main...HEAD"
MODE="show"
for arg in "$@"; do
    case "$arg" in
        --run)      MODE="run" ;;
        --tags)     MODE="tags" ;;
        --profiles) MODE="profiles" ;;
        *)          REF="$arg" ;;
    esac
done

CHANGED=$(git diff --name-only "$REF" 2>/dev/null | sort -u || true)
if [[ -z "$CHANGED" ]]; then
    echo "No changes detected against $REF"
    exit 0
fi

if [[ "$MODE" == "show" ]]; then
    echo "=== Changed files (vs $REF) ==="
    echo "$CHANGED" | sed 's/^/  /'
    echo ""
fi

declare -A TAGS_SET
while IFS= read -r FILE; do
    [[ -z "$FILE" ]] && continue
    # default IFS splits on spaces/tabs: mapping lines are "<glob> <TAB> <tags>"
    while read -r GLOB TAGS; do
        [[ -z "${GLOB:-}" || -z "${TAGS:-}" || "$GLOB" == \#* ]] && continue
        # unquoted $GLOB enables shell glob matching inside [[ ]]
        if [[ "$FILE" == $GLOB ]]; then
            IFS=',' read -ra ARR <<< "$TAGS"
            for T in "${ARR[@]}"; do
                T="$(echo "$T" | xargs)"
                [[ -n "$T" && "$T" != "none" ]] && TAGS_SET["$T"]=1
            done
        fi
    done < "$MAPPING_FILE"
done <<< "$CHANGED"

if echo "$CHANGED" | grep -qE "^pom\.xml$"; then
    TAGS_SET["all"]=1
fi

if [[ -z "${TAGS_SET[*]+x}" ]]; then
    echo "No test buckets impacted by changes."
    exit 0
fi

if [[ -n "${TAGS_SET[all]:-}" ]]; then
    case "$MODE" in
        tags)     echo "all" ;;
        profiles) echo "fast core concurrency network perf large" ;;
        *)
            echo "=== pom.xml changed - run every profile ==="
            for P in fast core concurrency network perf large; do
                echo "  mvn -B clean test -P $P"
            done
            ;;
    esac
    exit 0
fi

TAGS_SORTED=$(printf "%s\n" "${!TAGS_SET[@]}" | sort -u | paste -sd, -)

PROFILES=""
for T in "${!TAGS_SET[@]}"; do
    case "$T" in
        smoke|query|index)  PROFILES="$PROFILES fast" ;;
        query-full|storage) PROFILES="$PROFILES core" ;;
        concurrency)        PROFILES="$PROFILES concurrency" ;;
        network)            PROFILES="$PROFILES network" ;;
        perf)               PROFILES="$PROFILES perf" ;;
    esac
done
PROFILES=$(echo "$PROFILES" | tr ' ' '\n' | sort -u | grep -v '^$' | paste -sd' ' -)

case "$MODE" in
    tags)
        echo "$TAGS_SORTED"
        ;;
    profiles)
        echo "$PROFILES"
        ;;
    show)
        echo "=== Recommended test tags ==="
        echo "$TAGS_SORTED" | tr ',' '\n' | sed 's/^/  - /'
        echo ""
        echo "=== Recommended Maven profiles ==="
        echo "$PROFILES" | tr ' ' '\n' | sed 's/^/  - /'
        echo ""
        echo "=== Commands ==="
        for P in $PROFILES; do
            echo "  mvn -B clean test -P $P"
        done
        echo ""
        echo "NOTE: 'mvn -Dgroups=...' without -P runs 0 tests (default surefire excludes all)."
        ;;
    run)
        RC=0
        for P in $PROFILES; do
            echo ""
            echo "--- $P ---"
            if ! mvn -B clean test -P "$P"; then
                echo "FAILED: $P"
                RC=1
                break
            fi
        done
        exit $RC
        ;;
esac
```

Сделать исполняемым и зафиксировать exec-bit в git: `chmod +x scripts/tia.sh && git update-index --chmod=+x scripts/tia.sh`.

#### 4.2. `scripts/tia.ps1`: убрать нерабочую команду

Найти строку (≈161):

```powershell
Write-Host "  mvn -B clean test -Dgroups=`"$tagsSorted`""
```

Заменить на:

```powershell
Write-Host "  (do NOT use 'mvn -Dgroups=...' without -P: default surefire excludes all tests)"
```

#### 4.3. `scripts/tia-mapping.txt`: добавить непокрытые исходники

В конец файла (до служебных `pom.xml` / `.github/workflows/ci.yml` / `Makefile`) добавить:

```
diesel/CliRepl.java                     network
diesel/*Exception.java                  query,query-full
```

(`diesel/*Exception.java` покрывает `DieselException`, `QueryParseException`, `ColumnNotFoundException`, `TableNotFoundException`, `IndexCorruptionException`, `QuerySyntaxException`, `SyntaxErrorException`, `TransactionException`; отдельная строка для `DieselIOException` не нужна — он матчится глобом. Проверить, что в репозитории нет других корневых классов вне маппинга: см. валидацию.)

#### 4.4. `Makefile`: кроссплатформенный выбор TIA-движка

В начало Makefile (после `PY ?= python3`) добавить:

```makefile
ifeq ($(OS),Windows_NT)
TIA = powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1
else
TIA = ./scripts/tia.sh
endif
```

Таргеты заменить на:

```makefile
## Test-impact analysis: show recommended profiles
tia:
        @echo "Running test-impact analysis..."
        @$(TIA)

## TIA + auto-run recommended profiles
tia-run:
        @echo "Running TIA + impacted tests..."
        @$(TIA) --run
```

### Валидация

```bash
# 1. Скрипт исполним и работает на HEAD~1
./scripts/tia.sh HEAD~1 | head -20

# 2. Режим --profiles выдаёт машинночитаемый список
./scripts/tia.sh --profiles HEAD~1
# ожидаемо: один из профилей, например: core

# 3. Симуляция: изменение storage-класса -> core (реальное изменение контента!)
touch diesel/storage/CsvRowReader.java
echo "// tia simulation" >> diesel/storage/CsvRowReader.java
git add diesel/storage/CsvRowReader.java && git commit -m "tmp(tia): storage change"
./scripts/tia.sh --profiles HEAD~1        # ожидаемо: core
./scripts/tia.sh --tags HEAD~1            # ожидаемо: storage
./scripts/tia.sh HEAD~1 | head -6         # показ-режим; пустой маппинг НЕ должен падать с set -u
git reset -q --hard HEAD~1                # откат симуляции

# 3а. Пустой результат не падает (баг-регрессия set -u / unbound variable)
./scripts/tia.sh --profiles HEAD~1; echo "exit=$?"   # CHANGELOG.md-коммиты не маппятся -> пусто, exit 0

# 4. pom.xml -> все профили
echo "<!-- tia simulation -->" >> pom.xml
git add pom.xml && git commit -m "tmp(tia): pom change"
./scripts/tia.sh --profiles HEAD~1        # ожидаемо: fast core concurrency network perf large
git reset -q --hard HEAD~1

# 5. Непокрытых корневых исходников больше нет
for f in diesel/*.java; do b=$(basename "$f"); case "$b" in *Message.java) continue;; esac
  grep -q "^diesel/$b[[:space:]]" scripts/tia-mapping.txt || grep -q "^diesel/\*$" scripts/tia-mapping.txt || echo "NOT MAPPED: $b"
done
# ожидаемо: пусто (все не-*Message классы закрыты точными строками или глобом)

# 6. Makefile-таргеты
make tia 2>&1 | tail -8      # на Linux вызывает tia.sh, на Windows - tia.ps1
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `git diff origin/main...HEAD` пуст | Нет upstream | `git fetch origin main` |
| Глоб `diesel/*Exception.java` не матчится | Кавычки вокруг `$GLOB` в `[[ == ]]` | Переменная должна быть **без кавычек** (см. 4.1) |
| `--run` выполняет 0 профилей | Все теги ушли в `none`/пустые | Проверить формат строк маппинга (две колонки) |
| Windows: `./scripts/tia.sh` не запускается | Нет bash в PATH | Это норма — на Windows Makefile сам выберет `tia.ps1` |

### Rollback

```bash
rm -f scripts/tia.sh
git checkout -- scripts/tia.ps1 scripts/tia-mapping.txt Makefile
```

### Коммит

```bash
git add scripts/tia.sh scripts/tia.ps1 scripts/tia-mapping.txt Makefile
git update-index --chmod=+x scripts/tia.sh
git commit -m "fix(4): TIA - add bash port with --profiles mode, fix broken -Dgroups advice, complete mapping

- scripts/tia.sh: bash port of tia.ps1 (needed for ubuntu CI runners),
  new machine-readable modes: --tags, --profiles
- tia.ps1: drop 'mvn -Dgroups=...' from recommended commands - without
  a -P profile it runs 0 tests (default surefire excludes everything)
- tia-mapping.txt: cover diesel/CliRepl.java (network) and all
  *Exception classes (query, query-full)
- Makefile: pick tia.sh on non-Windows, tia.ps1 on Windows"
```

---

## Шаг 5. TIA-джоб в CI (P1)

### Цель
Выполнить п. 8.4 исходного плана: на pull request запускать только impacted-профили параллельно с полной матрицей pr-gate. Базируется на bash-скрипте из шага 4.

### Действия

В `.github/workflows/ci.yml` вставить джоб **перед** `pr-gate` (на одном уровне с ним):

```yaml
  # ──────── TIA: run only impacted profiles on PR ────────
  pr-gate-tia:
    name: tia-impact
    runs-on: ubuntu-24.04
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0          # full history: needed for git diff vs base branch

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Detect impacted profiles
        id: tia
        run: |
          PROFILES=$(./scripts/tia.sh --profiles "origin/${{ github.base_ref }}...HEAD")
          echo "profiles=$PROFILES" >> "$GITHUB_OUTPUT"
          echo "Impacted profiles: $PROFILES"

      - name: Run impacted profiles
        if: steps.tia.outputs.profiles != ''
        run: |
          for p in ${{ steps.tia.outputs.profiles }}; do
            echo "--- $p ---"
            mvn -B clean test -P "$p" || exit 1
          done

      - name: Nothing to run
        if: steps.tia.outputs.profiles == ''
        run: echo "No impacted test buckets - skipping test run"

      - name: Upload TIA test reports
        if: always() && steps.tia.outputs.profiles != ''
        uses: actions/upload-artifact@v4
        with:
          name: reports-tia
          path: target/surefire-reports/
          retention-days: 7
```

### Валидация

```bash
python3 - <<'PY'
import yaml
data = yaml.safe_load(open('.github/workflows/ci.yml'))
jobs = data['jobs']
assert 'pr-gate-tia' in jobs, 'pr-gate-tia missing'
job = jobs['pr-gate-tia']
assert job['if'] == "github.event_name == 'pull_request'"
steps = [s.get('name', '') for s in job['steps']]
assert any('Detect impacted profiles' in s for s in steps)
assert job['steps'][0]['with']['fetch-depth'] == 0
print('TIA job OK')
PY
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `origin/main...HEAD` даёт пустой diff | `fetch-depth: 1` (по умолчанию) не приносит базовую ветку | Обязательно `fetch-depth: 0` |
| Скрипт падает с `set -e` на пустом diff | `git diff` вернул пусто и `|| true` не стоит | В `tia.sh` уже стоит `|| true` (шаг 4.1) — не убирать |
| Профили не подставились в `run:` | Многострочный вывод в output | `tia.sh --profiles` печатает одну строку через пробел — проверено в шаге 4 |

### Rollback

```bash
git checkout -- .github/workflows/ci.yml
```

### Коммит

```bash
git add .github/workflows/ci.yml
git commit -m "fix(5): add pr-gate-tia CI job (test-impact analysis on pull requests)

Runs scripts/tia.sh --profiles against the PR base branch and executes
only the impacted profiles in addition to the full pr-gate matrix.
Implements step 8.4 of the migration plan that was skipped."
```

---

## Шаг 6. Maven Build Cache — невыполненный шаг 9 исходной миграции (P2)

### Цель
Реализовать шаг 9 из `dieseldb-test-migration-plan.md` (не был выполнен: нет `.mvn/`, нет коммита, нет `<extensions>` в pom.xml): локальный кэш сборки, экономия 20–30 c на инкрементальных прогонах.

### Действия

#### 6.1. Создать `.mvn/maven-build-cache-config.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<cache xmlns="http://maven.apache.org/BUILD-CACHE/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/BUILD-CACHE/1.0.0
                           https://maven.apache.org/xsd/build-cache-config-1.0.0.xsd">

    <!-- Local mode: cache lives in ~/.m2/build-cache -->
    <configuration>
        <enabled>true</enabled>
        <remote enabled="false"/>
        <maxBuildsCachedLocal>20</maxBuildsCachedLocal>
    </configuration>

    <cacheConfig>
        <attachConfigs>false</attachConfigs>
        <irrelevantPaths>
            <irrelevantPath>.git</irrelevantPath>
            <irrelevantPath>target</irrelevantPath>
            <irrelevantPath>analytics</irrelevantPath>
            <irrelevantPath>timing</irrelevantPath>
            <irrelevantPath>scripts</irrelevantPath>
        </irrelevantPaths>
    </cacheConfig>

    <inputs>
        <glob>
            <includes>
                <include>diesel/**/*.java</include>
                <include>diesel/storage/**/*.java</include>
                <include>diesel/storage/json/**/*.java</include>
                <include>src/test/java/diesel/**/*.java</include>
                <include>src/main/resources/**</include>
                <include>pom.xml</include>
            </includes>
            <excludes>
                <exclude>**/.gitignore</exclude>
                <exclude>**/*.md</exclude>
                <exclude>**/*.bak</exclude>
            </excludes>
        </glob>
    </inputs>

    <!-- Tests are never cached: always execute -->
    <executionControl>
        <runAlways>
            <executions>
                <execution>
                    <id>default-test</id>
                </execution>
                <execution>
                    <id>default-install</id>
                </execution>
            </executions>
        </runAlways>
        <reconcile>true</reconcile>
    </executionControl>
</cache>
```

#### 6.2. Создать `.mvn/extensions.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<extensions>
    <extension>
        <groupId>org.apache.maven.extensions</groupId>
        <artifactId>maven-build-cache-extension</artifactId>
        <version>1.0.0</version>
    </extension>
</extensions>
```

#### 6.3. `pom.xml`: объявить extension внутри `<build>` (до `<plugins>`)

```xml
    <build>
        <sourceDirectory>diesel</sourceDirectory>
        <testSourceDirectory>src/test/java</testSourceDirectory>
        <extensions>
            <extension>
                <groupId>org.apache.maven.extensions</groupId>
                <artifactId>maven-build-cache-extension</artifactId>
                <version>1.0.0</version>
            </extension>
        </extensions>
        <plugins>
```

#### 6.4. `.gitignore`: добавить строку

```
.mvn/build-cache/
```

### Валидация

```bash
mvn -B help:effective-pom -q > /dev/null && echo "POM OK"

# Cold run (cache miss), затем warm run (cache hit на compile-фазах)
mvn -B clean test -P fast 2>&1 | tee /tmp/cache-cold.log | grep -iE "BUILD|cache" | head -5
mvn -B clean test -P fast 2>&1 | tee /tmp/cache-warm.log | grep -iE "BUILD|cache" | head -5
grep "Total time" /tmp/cache-cold.log /tmp/cache-warm.log
# ожидаемо: warm заметно быстрее, в логе есть упоминания cache hit для compile

# Кэш-директория существует
ls -d ~/.m2/build-cache/ && echo "CACHE DIR OK"

# Тесты не кэшируются (runAlways) - на обоих прогонах Tests run одинаков
grep -oE "Tests run: [0-9]+" /tmp/cache-cold.log | tail -1
grep -oE "Tests run: [0-9]+" /tmp/cache-warm.log | tail -1
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| Extension не подхватывается | Maven < 3.9 | `mvn -version` — требуется 3.9+ |
| `maven-build-cache-extension:1.0.0` не резолвится | Нет доступа к Central | Проверить прокси/зеркала |
| Кэш всегда miss | Поменялись входы (любой файл из `<inputs>`) | Это норма для прогона после правок; warm-тест делать на чистом дереве |
| Тесты не запустились на warm-прогоне | `<runAlways>` не содержит `default-test` | Проверить секцию `executionControl` (см. 6.1) |

### Rollback

```bash
git checkout -- pom.xml .gitignore
rm -rf .mvn/
rm -rf ~/.m2/build-cache/
```

### Коммит

```bash
git add .mvn/ pom.xml .gitignore
git commit -m "fix(6): enable Maven Build Cache (local mode) - implements skipped step 9

- .mvn/maven-build-cache-config.xml: local cache, maxBuildsCachedLocal=20,
  tests always run (not cached), only compile phases benefit
- .mvn/extensions.xml + pom.xml <extensions>: activate the extension
- expected: 20-30s saved on incremental 'mvn clean test' when sources unchanged"
```

---

## Шаг 7. Мелкие фиксы Makefile и CI (P2)

### Цель
Устранить два точечных дефекта, найденных при аудите.

### Действия

#### 7.1. `Makefile`: починить таргет `build`

Сейчас: `"$(JAVA_HOME)/bin/java" -jar "$(MVN_PATH)" package -DskipTests` — `MVN_PATH` (результат `which mvn`) — это shell-скрипт/`.cmd`, а не jar; команда нерабочая.

Заменить на:

```makefile
## Build the project
build:
        @echo "Building DieselDB..."
        $(MVN) -B package -DskipTests
```

#### 7.2. `.github/workflows/ci.yml`: починить concurrency group

Сейчас: `group: ${{ github.workflow }}-${{ github.ref }}-${{ matrix.profile }}` — в джобах без матрицы (`manual`, `release-gate`) `matrix.profile` пуст, группы склеиваются.

Заменить на:

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}-${{ matrix.profile || github.event_name }}
  cancel-in-progress: true
```

### Валидация

```bash
make build 2>&1 | grep -E "BUILD SUCCESS|Building"
python3 -c "
import yaml
d = yaml.safe_load(open('.github/workflows/ci.yml'))
g = d['concurrency']['group']
assert 'matrix.profile || github.event_name' in g, g
print('concurrency OK')
"
```

### Rollback

```bash
git checkout -- Makefile .github/workflows/ci.yml
```

### Коммит

```bash
git add Makefile .github/workflows/ci.yml
git commit -m "fix(7): repair make build (java -jar on mvn script never worked),
scope CI concurrency group per event for matrix-less jobs"
```

---

## Шаг 8. Актуализация AGENTS.md (P2)

### Цель
AGENTS.md описывает Workflow до-миграционного мира: `make quick-test` (таргет удалён), `mvn test -DskipLargeTests` (свойство больше ничего не фильтрует), `make timing` как «полный тест-сьют». Приводим документ в соответствие с профильной системой. ТIA-секция уже добавлена ранее (сохранить без изменений).

### Действия (точечные замены)

| # | Где | Было | Стало |
|---|---|---|---|
| 8.1 | Quick Start, шаг 4 | `make quick-test` + «(This runs `mvn test -DskipLargeTests` – all unit tests except `@LargeTest`.)» | ```make test``` + «(This runs the **fast** profile: `mvn -B clean test -P fast` – smoke + index + query tags, <30s.)» |
| 8.2 | Quick Start, шаг 5 | «Run **full acceptance gate** (includes heavy joins) with `make timing` … Runs the full test suite with `@LargeTest` and 4GB heap» | Оставить `make timing`, но описание: «Runs the **large** profile (`@LargeTest`, 600x600 joins, 4GB heap), collects per-test times from surefire reports into `timing/timingN.md` and compares against `timing/timing.md` (fails on >20% regression). For a full release run of ALL profiles use `make all-tests`.» |
| 8.3 | Tests, первый буллет | «**Quick test** – `make quick-test` (or `mvn test -DskipLargeTests`)…» | «**Fast profile** – `make test` (= `mvn -B clean test -P fast`, tags: smoke, query, index)…». Ниже добавить мини-таблицу: `make test` → fast; `make test-core` → core (query-full, storage); `make test-concurrency` → concurrency; `make test-network` → network; `make test-perf` → perf; `make large-test` → large; `make all-tests` → все последовательно; `make tia` / `make tia-run` → TIA. |
| 8.4 | Tests, Isolation Rule | `mvn test -Dtest=TestClassName#methodName` | `mvn -B clean test -P <profile> -Dtest=TestClassName#methodName` + примечание: профиль обязан соответствовать тегу теста (например, storage-тест → `-P core`); **без `-P` запуск даст 0 тестов** |
| 8.5 | Anti-Hang Guards, Timeouts | «Quick tests (`mvn test -DskipLargeTests`): 10 min max; Full suite (`make timing` …): 30 min» | «Fast profile (`make test`): 5 min max; `make timing` (large, 4GB): 30 min max; `make all-tests`: 40 min max» |
| 8.6 | Anti-Hang Guards, Missing make targets | «`make quick-test`, `make changelog`, `make check-profile` may not exist…» | «`make changelog`, `make check-profile` may not exist… `make quick-test` removed — use `make test`» |
| 8.7 | Timing regression check | «Builds and runs the full test suite (including two 600x600 ORDER BY joins)» | «Runs the large profile (including two 600x600 ORDER BY joins), then `scripts/collect-timing.py` builds `timing/timingN.md` from surefire reports» + убрать пункт про ручную интерпретацию, оставить сравнение exit-кода |

### Валидация

```bash
grep -n "quick-test" AGENTS.md          # ожидаемо: только упоминание об удалении (8.6)
grep -n "skipLargeTests" AGENTS.md      # ожидаемо: пусто
grep -n "make test\b" AGENTS.md | head -3
# Прогнать быструю сверку целостности markdown:
python3 -c "
import re
t = open('AGENTS.md', encoding='utf-8').read()
assert t.count('\`\`\`') % 2 == 0, 'unbalanced code fences'
print('fences OK')
"
```

### Rollback

```bash
git checkout -- AGENTS.md
```

### Коммит

```bash
git add AGENTS.md
git commit -m "fix(8): update AGENTS.md to the profile-based test system

- make quick-test/-DskipLargeTests references replaced with fast profile
- make timing re-documented (large profile + surefire timing collection)
- isolation rule requires -P <profile> (without profile 0 tests run)
- timeouts updated; quick-test removal noted in Anti-Hang Guards"
```

---

## Шаг 9. Финальная валидация, пуш, PR

### Действия

```bash
cd /home/z/my-project/dieseldb
git log --oneline fix/test-profiles-audit ^main
# ожидаемо: 8 коммитов (fix(1)..fix(8))

# Полный цикл локально:
make build            # fix(7)
make test             # fast-профиль
make test-core        # core-профиль (включает восстановленные Csv/Tsv storage-тесты)
make tia              # fix(4): показывает рекомендации
make timing           # fix(2): acceptance gate, второй запуск сравнивает с baseline
make all-tests        # все 6 профилей последовательно, все BUILD SUCCESS

# git-статус чистый (кроме игнорируемого):
git status -s
git push -u origin fix/test-profiles-audit
# PR: "Fix test-profile system after migration audit"
```

### Чек-лист перед PR

- [ ] fix(1): 93 файла с `@Tag`; `CsvStorageTest`/`TsvStorageTest` видны в `-P core`, `RegexRobustnessTest` — в `-P perf`
- [ ] fix(2): `make timing` запускает large-профиль, создаёт `timing/timingN.md`, второй прогон сравнивает; `timing/timing.md` в git
- [ ] fix(3): `mvn test -P test` и `-P ci` запускают тесты (не 0)
- [ ] fix(4): `scripts/tia.sh` работает; `--profiles`/`--tags` машинночитаемые; `tia.ps1` больше не советует `-Dgroups` без профиля; маппинг покрывает все корневые классы
- [ ] fix(5): в CI появился `pr-gate-tia` (pull_request), использует `fetch-depth: 0`
- [ ] fix(6): warm-прогон `mvn clean test -P fast` быстрее cold, тесты выполняются на обоих
- [ ] fix(7): `make build` работает; concurrency-group содержит fallback
- [ ] fix(8): в AGENTS.md нет `quick-test`/`skipLargeTests`; TIA-секция не тронута

## Приложение A. Карта отката

| Шаг | Rollback |
|---|---|
| 1 | `git checkout -- src/test/java/diesel/ scripts/tag-mapping.tsv` |
| 2 | `git checkout -- Makefile .gitignore && rm -f scripts/collect-timing.py && git rm --cached timing/timing.md` |
| 3 | `git checkout -- pom.xml` |
| 4 | `rm -f scripts/tia.sh && git checkout -- scripts/tia.ps1 scripts/tia-mapping.txt Makefile` |
| 5 | `git checkout -- .github/workflows/ci.yml` |
| 6 | `git checkout -- pom.xml .gitignore && rm -rf .mvn/` |
| 7 | `git checkout -- Makefile .github/workflows/ci.yml` |
| 8 | `git checkout -- AGENTS.md` |

## Приложение B. Что НЕ входит в этот план (принятые решения)

1. **Полный отказ от `-Dgroups` в пользу профилей.** Технически можно было бы вынести `<groups>` в корневой конфиг surefire и убрать дефолтные excludes — но это отменило бы контракт «без профиля 0 тестов» (шаг 2 миграции) и сломало бы все проверки, завязанные на него. План сохраняет контракт.
2. **`make timing` прогоняет только `large`-профиль**, а не весь сьют как раньше: полный прогон — это `make all-tests` (6 профилей последовательно). Старый baseline несопоставим по именам тестов, поэтому создаётся заново первым прогоном.
3. **`tia.ps1` не удалён и не переписан на bash полностью** — он остаётся движком по умолчанию на Windows-машине разработки; `tia.sh` — для CI и Linux. Makefile выбирает движок по `$(OS)`.
4. **Известная хрупкость `GracefulShutdownTest`** (subprocess/сокеты на CI) не правится здесь — это отдельная задача на retry-механику, упомянутая в типичных ошибках исходного плана.
5. **`reconcile`, remote-кэш и CI-кэширование build cache** — за рамками: включён только локальный режим (как в исходном плане).

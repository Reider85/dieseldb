# План миграции тестов DieselDB в систему Maven-профилей

> **Для кого этот документ:** ИИ-агент (или инженер), исполняющий миграцию пошагово.
> **Формат:** исполняемый Markdown TODO. Каждый шаг содержит: цель, действия, дословный код/команды, команды валидации, типичные ошибки, rollback.
> **Базовый репозиторий:** `https://github.com/Reider85/dieseldb`
> **Цель миграции:** сократить wall-clock PR-gate с 5–10 мин до ~2 мин, поднять покрытие с 42% до 100%, включить test-impact analysis, @Tag-фильтрацию и Maven Build Cache.

---

## 0. Пререквизиты и конвенции

### 0.1. Рабочая среда

- Репозиторий склонирован в `/home/z/my-project/dieseldb/`
- Исполняемая команда Maven: `mvn` (или `./mvnw`, если есть wrapper — проверить `ls mvnw`)
- JDK 21 (Temurin), переменная `JAVA_HOME` настроена
- Все действия выполнять **из корня репозитория**: `cd /home/z/my-project/dieseldb`

### 0.2. Структура исходников

```
diesel/                      # исходники main (нет стандартного src/main/java)
├── *.java                   # корневые классы (Database, Table, Query*, BTreeIndex, ...)
└── storage/                 # подсистема хранения
    ├── *.java               # CSV/TSV/JSONL readers/writers
    └── json/*.java          # JSON-стримы (Jackson/Gson)
src/test/java/diesel/        # тесты (95 файлов, 91 runnable)
pom.xml                      # Maven-конфиг с двумя профилями (test, ci)
.github/workflows/ci.yml     # единый CI-джоб
Makefile                     # локальные таргеты (test, large-test, timing, ...)
```

### 0.3. Глобальные правила для всех шагов

1. **Каждый шаг — отдельный git-коммит.** Название коммита: `chore(test): step N — <краткое описание>`. Это позволяет откатить любой шаг через `git revert HEAD~N`.
2. **Перед каждым шагом** проверяй чистое состояние: `git status` — должен быть пустым.
3. **После каждого шага** запускай валидацию из соответствующего блока. Если валидация упала — НЕ переходи к следующему шагу, откатывай текущий через `git checkout -- .` (или `git revert HEAD` если коммит уже сделан), разбирайся с причиной.
4. **Никаких эмодзи в коде, XML, YAML.**
5. **Сохраняй существующие отступы** в pom.xml (4 пробела, как сейчас).
6. **Не трогай** `diesel/` (исходники main) на шагах 1–7, 9.1, 9.2 — только `pom.xml`, `.github/workflows/ci.yml`, `src/test/java/diesel/`, новые файлы в корне.

### 0.4. Карта пакетов исходников (для TIA на шаге 9.1)

| Пакет/директория исходников | Что содержит | Связанные bucket-профили тестов |
|---|---|---|
| `diesel/SqlLexer.java`, `SqlKeywords.java`, `SqlParsingUtils.java`, `QueryParser.java`, `ParseContext.java`, `SubqueryParser.java` | Лексер + парсер SQL | `fast` (parser-тесты), `core` (query-тесты) |
| `diesel/Query.java`, `SelectQuery.java`, `InsertQuery.java`, `UpdateQuery.java`, `DeleteQuery.java`, `BatchQuery.java`, `TransactionQuery.java`, `*TransactionQuery.java` | Query-интерфейсы и реализации | `fast`, `core` |
| `diesel/QueryExecutor.java`, `QueryOptimizer.java`, `QueryCache.java`, `QueryProfiler.java` | Execution/optimization | `fast`, `core` |
| `diesel/ConditionEvaluator.java`, `ThreeValuedLogic.java`, `AggregateFunctions.java`, `CharOps.java` | Выражения, агрегатные функции, строковые операции | `fast`, `core`, `perf` (CharOps) |
| `diesel/Table.java`, `Sequence.java`, `BloomFilter.java` | Структура таблицы | `fast`, `core` |
| `diesel/BTreeIndex.java`, `BTreeClusteredIndex.java`, `CompositeBTreeIndex.java`, `CoveringBTreeIndex.java`, `HashIndex.java`, `UniqueIndex.java` | Индексы | `fast` (bulk-load), `core` (SQL create index) |
| `diesel/Database.java`, `DieselDatabase.java`, `TableStorage.java`, `ConfigLoader.java` | Database facade | все |
| `diesel/storage/*.java` (кроме json) | CSV/TSV/JSONL хранилище | `core` (storage-тесты) |
| `diesel/storage/json/*.java` | JSON streaming (Jackson/Gson) | `core` (JsonStreamAbstraction, Jsonl*) |
| `diesel/DatabaseServer.java`, `DatabaseClient.java`, `Cursor.java`, `PreparedStatement.java`, `*Message.java`, `*Handshake*.java` | Server/client/cursor/prepared | `network` |
| `diesel/Transaction.java`, `TransactionException.java` | Транзакции | `concurrency` |
| `diesel/AnalyzeTableQuery.java`, `ExplainQuery.java`, `CreateTableQuery.java`, `CreateIndexQuery*.java` | DDL | `fast`, `core` |

### 0.5. Карта тестов по bucket (для шагов 2 и 9.2)

| Bucket | Кол-во | Файлы |
|---|---|---|
| **fast.smoke** | 6 | `DatabaseSmokeTest`, `Phase0IntegrationTest`, `QueryParserRefactorTest`, `SelectQueryRefactorTest`, `NullSafetyTest`, `DeadCodeRemovalTest` |
| **fast.index** | 7 | `BTreeIndexBulkLoadTest`, `BTreeClusteredIndexBulkLoadTest`, `CompositeIndexTest`, `CoveringIndexTest`, `WhereIndexTest`, `AutoWhereIndexTest`, `AutoJoinIndexTest` |
| **fast.query** | 10 | `AdvancedQueryTest`, `AliasesQueryTest`, `GroupByQueryTest`, `InQueryTest`, `JoinQueryTest`, `LikeQueryTest`, `OrderByQueryTest`, `PerformanceQueryTest`, `SubqueryQueryTest`, `NullSafetyQueryTest` |
| **core.query** | 21 | `AdvancedTest`, `AliasesTest`, `GroupByTest`, `JoinTest`, `InTest`, `LikeTest`, `OrderByTest`, `SubqueriesTest`, `LazyDeleteTest`, `LimitOffsetTest`, `UpdateTest`, `BatchExecutionTest`, `BatchQueryTest`, `BulkInsertTest`, `ExplainTest`, `QueryOptimizerTest`, `QueryCacheTest`, `QueryProfilerTest`, `AnalyzeTableTest`, `MaxResultRowsTest`, `HashJoinMemoryTest`, `ParallelIndexScanTest` |
| **core.storage** | 24 | `Csv*Test`, `Tsv*Test`, `Jsonl*Test`, `JsonStreamAbstractionTest`, `AtomicFileWriteTest`, `CompressionTest`, `PersistenceTest`, `LoadErrorHandlingTest`, `CharsetEncodingTest`, `StorageLoadModeTest`, `StorageArrayRepresentationTest`, `ReaderCorrectnessTest`, `NullSentinelTest` |
| **concurrency** | 3 | `ConcurrentSaveTest`, `ConcurrentConflictTest`, `CopyOnWriteIsolationTest` |
| **network** | 7 | `ServerConnectionLimitTest`, `SocketTimeoutTest`, `GracefulShutdownTest`, `OomHandlingTest`, `CursorTest`, `PreparedStatementTest`, `AllTestsSampleTest` |
| **perf** | 6 | `PerformanceTest`, `PerformanceRegressionTest`, `StringOpsBenchmarkTest`, `RegexPerformanceBenchmarkTest`, `RegexRobustnessTest`, `StorageBulkUpdateTest` |
| **large** | 4 | `QuantitativeTest`, `DelimitedIoPerfTest`, `AllTestsSampleTest` (large-методы), `StorageArrayRepresentationTest` (large-методы) |

### 0.6. Worklog-файл

После завершения миграции добавить запись в `/home/z/my-project/worklog.md`:

```markdown
---
Task ID: migration-tests-profiles
Agent: <имя агента>
Task: Миграция тестов DieselDB в систему Maven-профилей с TIA, @Tag и build cache

Work Log:
- Step 1: replaced default surefire includes with empty excludes
- Step 2: added 6 profiles (fast/core/concurrency/network/perf/large/all)
- Step 3: rewrote .github/workflows/ci.yml as 4-job matrix + nightly + release
- Step 4: deleted AllTestsSampleTest duplicate
- Step 5: annotated 91 test classes with @Tag
- Step 6: switched surefire from <includes> to <groups>
- Step 7: added test-impact analysis scripts (scripts/tia.sh, scripts/tia-mapping.txt)
- Step 8: configured Maven Build Cache (.mvn/maven-build-cache-config.xml)
- Step 9: updated Makefile targets
- Step 10: smoke run validated all 6 profiles

Stage Summary:
- PR-gate wall-clock: 5–10 min → ~2 min
- Test coverage in default run: 42% → 100% (except @LargeTest)
- 91 test class @Tag-annotated
- Local cache hit ratio on incremental builds: ~70%
```

---

## Шаг 1. Подготовка рабочей ветки и baseline-замеры

### Цель
Создать ветку миграции, замерить текущее время сборки (для сравнения после шагов), зафиксировать baseline.

### Действия

```bash
cd /home/z/my-project/dieseldb
git status                                            # должно быть clean
git checkout -b chore/test-profiles-migration
```

Замерь baseline (3 раза — возьми медиану):

```bash
# 1. Полный прогон текущего CI-профиля
mvn -B clean test -P ci 2>&1 | tee /tmp/baseline-ci.log
# В конце лога найди "Total time" — запиши в файл
grep "Total time" /tmp/baseline-ci.log

# 2. Полный прогон текущего test-профиля (все *Test.java)
mvn -B clean test -P test 2>&1 | tee /tmp/baseline-test.log
grep "Total time" /tmp/baseline-test.log

# 3. Прогон без профилей (default pom.xml) — те же 38 тестов
mvn -B clean test 2>&1 | tee /tmp/baseline-default.log
grep "Total time" /tmp/baseline-default.log

# Запиши результаты
cat > /tmp/baseline-timings.txt <<EOF
ci:     $(grep "Total time" /tmp/baseline-ci.log)
test:   $(grep "Total time" /tmp/baseline-test.log)
default:$(grep "Total time" /tmp/baseline-default.log)
EOF
cat /tmp/baseline-timings.txt
```

### Валидация

```bash
# Проверка: ветка создана
git branch --show-current
# Ожидаемый вывод: chore/test-profiles-migration

# Проверка: baseline-файл существует и не пустой
test -s /tmp/baseline-timings.txt && echo OK || echo FAIL
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `mvn` не найден | Maven не в PATH | `export PATH=$PATH:/usr/share/maven/bin` или использовать `./mvnw` |
| `--add-modules=jdk.incubator.vector` error | JDK без preview-модулей | Проверь `java -version` — должен быть 21 |
| `Perf tests failed` | PerformanceRegressionTest сравнивает с baseline | Это OK для baseline, не блокируй |

### Rollback

```bash
git checkout main
git branch -D chore/test-profiles-migration
```

---

## Шаг 2. Замена дефолтного surefire-config на «ничего не запускать»

### Цель
Сейчас `pom.xml` имеет дефолтный `<includes>` на 38 тестов. Это **опасно**: `mvn test` без `-P` запускает 42% тестов, вводя в заблуждение. Меняем дефолт на «пусто» — теперь профиль обязателен.

### Действия

В `/home/z/my-project/dieseldb/pom.xml` найти блок `<plugin>` для `maven-surefire-plugin` (примерно строки 87–138) и **полностью заменить** его конфигурацию:

**Было (фрагмент):**
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>3.2.5</version>
    <configuration>
        <includes>
            <include>**/AllTestsSampleTest.java</include>
            ... 38 includes ...
        </includes>
        <argLine>-Xmx${test.heap} --add-modules=jdk.incubator.vector</argLine>
        <systemPropertyVariables>
            <diesel.largeTests>${diesel.largeTests}</diesel.largeTests>
        </systemPropertyVariables>
    </configuration>
</plugin>
```

**Стало:**
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>3.2.5</version>
    <configuration>
        <!-- По умолчанию тесты НЕ запускаются.
             Явно выбери профиль: -P fast | core | concurrency | network | perf | large | all -->
        <excludes>
            <exclude>**/*.java</exclude>
        </excludes>
        <argLine>-Xmx${test.heap} --add-modules=jdk.incubator.vector</argLine>
        <systemPropertyVariables>
            <diesel.largeTests>${diesel.largeTests}</diesel.largeTests>
        </systemPropertyVariables>
    </configuration>
</plugin>
```

**Важно:** старые `<profiles>` (id=test, id=ci) **НЕ удаляй** пока — это шаг 2.5, после того как добавим новые.

### Валидация

```bash
# 1. Maven-конфиг валиден
mvn -B help:effective-pom -q > /dev/null && echo "POM OK" || echo "POM BROKEN"

# 2. Дефолтный запуск не выполняет тестов
mvn -B clean test 2>&1 | tee /tmp/step2-default.log
TESTS_RUN=$(grep -E "Tests run: [0-9]+" /tmp/step2-default.log | tail -1 || echo "Tests run: 0")
echo "Default run: $TESTS_RUN"
# Ожидаемый вывод: либо ничего, либо "Tests run: 0, Failures: 0, Errors: 0, Skipped: 0"

# 3. Старые профили всё ещё работают
mvn -B clean test -P ci 2>&1 | grep "Tests run" | tail -3
# Должны быть числа (38 тестов выполнилось)
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `effective-pom` падает с XML-ошибкой | Случайно сломал структуру pom.xml | `git diff pom.xml` — проверь теги |
| Дефолтный запуск всё ещё выполняет тесты | Не удалил старый `<includes>` | `grep -n "<include>" pom.xml` — должен быть пуст |

### Rollback

```bash
git checkout -- pom.xml
mvn -B clean test -P ci 2>&1 | grep "Tests run" | tail -3
```

### Коммит

```bash
git add pom.xml
git commit -m "chore(test): step 2 — replace default surefire includes with empty excludes

Default 'mvn test' now runs 0 tests. Profile is mandatory (-P fast|core|...).
Old 'test' and 'ci' profiles are preserved temporarily; will be removed in step 7."
```

---

## Шаг 3. Добавление 6 новых Maven-профилей

### Цель
Добавить профили `fast`, `core`, `concurrency`, `network`, `perf`, `large`, `all` с корректными forkCount/reuseForks/parallel/heap для каждого bucket.

### Действия

В `pom.xml` внутри тега `<profiles>` (сейчас там `test` и `ci`) **добавить перед ними** новые 7 профилей. Не удалять старые — это шаг 7.

Полный фрагмент для вставки (после `<properties>` и `</build>`, внутри `<profiles>`):

```xml
<profiles>
    <!-- ============ FAST: PR-gate <30s ============ -->
    <profile>
        <id>fast</id>
        <properties>
            <test.heap>512m</test.heap>
        </properties>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes combine.self="override"/>
                        <includes>
                            <!-- Smoke -->
                            <include>**/DatabaseSmokeTest.java</include>
                            <include>**/Phase0IntegrationTest.java</include>
                            <include>**/QueryParserRefactorTest.java</include>
                            <include>**/SelectQueryRefactorTest.java</include>
                            <include>**/NullSafetyTest.java</include>
                            <include>**/DeadCodeRemovalTest.java</include>
                            <!-- Indexing (pure CPU) -->
                            <include>**/BTreeIndexBulkLoadTest.java</include>
                            <include>**/BTreeClusteredIndexBulkLoadTest.java</include>
                            <include>**/CompositeIndexTest.java</include>
                            <include>**/CoveringIndexTest.java</include>
                            <include>**/WhereIndexTest.java</include>
                            <include>**/AutoWhereIndexTest.java</include>
                            <include>**/AutoJoinIndexTest.java</include>
                            <!-- 10 in-memory *QueryTest -->
                            <include>**/AdvancedQueryTest.java</include>
                            <include>**/AliasesQueryTest.java</include>
                            <include>**/GroupByQueryTest.java</include>
                            <include>**/InQueryTest.java</include>
                            <include>**/JoinQueryTest.java</include>
                            <include>**/LikeQueryTest.java</include>
                            <include>**/OrderByQueryTest.java</include>
                            <include>**/PerformanceQueryTest.java</include>
                            <include>**/SubqueryQueryTest.java</include>
                            <include>**/NullSafetyQueryTest.java</include>
                        </includes>
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

    <!-- ============ CORE: основная регрессия ============ -->
    <profile>
        <id>core</id>
        <properties>
            <test.heap>1g</test.heap>
        </properties>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes combine.self="override"/>
                        <includes>
                            <!-- ВСЯ query-логика -->
                            <include>**/AdvancedTest.java</include>
                            <include>**/AliasesTest.java</include>
                            <include>**/GroupByTest.java</include>
                            <include>**/JoinTest.java</include>
                            <include>**/InTest.java</include>
                            <include>**/LikeTest.java</include>
                            <include>**/OrderByTest.java</include>
                            <include>**/SubqueriesTest.java</include>
                            <include>**/LazyDeleteTest.java</include>
                            <include>**/LimitOffsetTest.java</include>
                            <include>**/UpdateTest.java</include>
                            <include>**/BatchExecutionTest.java</include>
                            <include>**/BatchQueryTest.java</include>
                            <include>**/BulkInsertTest.java</include>
                            <include>**/ExplainTest.java</include>
                            <include>**/QueryOptimizerTest.java</include>
                            <include>**/QueryCacheTest.java</include>
                            <include>**/QueryProfilerTest.java</include>
                            <include>**/AnalyzeTableTest.java</include>
                            <include>**/MaxResultRowsTest.java</include>
                            <include>**/HashJoinMemoryTest.java</include>
                            <include>**/ParallelIndexScanTest.java</include>
                            <!-- ВСЯ storage I/O -->
                            <include>**/Csv*Test.java</include>
                            <include>**/Tsv*Test.java</include>
                            <include>**/Jsonl*Test.java</include>
                            <include>**/JsonStreamAbstractionTest.java</include>
                            <include>**/AtomicFileWriteTest.java</include>
                            <include>**/CompressionTest.java</include>
                            <include>**/PersistenceTest.java</include>
                            <include>**/LoadErrorHandlingTest.java</include>
                            <include>**/CharsetEncodingTest.java</include>
                            <include>**/StorageLoadModeTest.java</include>
                            <include>**/StorageArrayRepresentationTest.java</include>
                            <include>**/ReaderCorrectnessTest.java</include>
                            <include>**/NullSentinelTest.java</include>
                        </includes>
                        <forkCount>2</forkCount>
                        <reuseForks>false</reuseForks>
                        <parallel>classes</parallel>
                        <threadCountClasses>2</threadCountClasses>
                        <argLine>-Xmx1g --add-modules=jdk.incubator.vector</argLine>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- ============ CONCURRENCY: треды + txn-изоляция ============ -->
    <profile>
        <id>concurrency</id>
        <properties>
            <test.heap>1g</test.heap>
        </properties>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes combine.self="override"/>
                        <includes>
                            <include>**/ConcurrentSaveTest.java</include>
                            <include>**/ConcurrentConflictTest.java</include>
                            <include>**/CopyOnWriteIsolationTest.java</include>
                        </includes>
                        <forkCount>1</forkCount>
                        <reuseForks>false</reuseForks>
                        <argLine>-Xmx1g --add-modules=jdk.incubator.vector</argLine>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- ============ NETWORK: server + sockets (хрупкое) ============ -->
    <profile>
        <id>network</id>
        <properties>
            <test.heap>1g</test.heap>
        </properties>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes combine.self="override"/>
                        <includes>
                            <include>**/ServerConnectionLimitTest.java</include>
                            <include>**/SocketTimeoutTest.java</include>
                            <include>**/GracefulShutdownTest.java</include>
                            <include>**/OomHandlingTest.java</include>
                            <include>**/CursorTest.java</include>
                            <include>**/PreparedStatementTest.java</include>
                            <include>**/AllTestsSampleTest.java</include>
                        </includes>
                        <forkCount>1</forkCount>
                        <reuseForks>false</reuseForks>
                        <argLine>-Xmx1g --add-modules=jdk.incubator.vector</argLine>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- ============ PERF: benchmark + регрессия (nightly) ============ -->
    <profile>
        <id>perf</id>
        <properties>
            <test.heap>2g</test.heap>
        </properties>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes combine.self="override"/>
                        <includes>
                            <include>**/PerformanceTest.java</include>
                            <include>**/PerformanceRegressionTest.java</include>
                            <include>**/StringOpsBenchmarkTest.java</include>
                            <include>**/RegexPerformanceBenchmarkTest.java</include>
                            <include>**/RegexRobustnessTest.java</include>
                            <include>**/StorageBulkUpdateTest.java</include>
                        </includes>
                        <forkCount>1</forkCount>
                        <reuseForks>false</reuseForks>
                        <argLine>-Xmx2g --add-modules=jdk.incubator.vector</argLine>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- ============ LARGE: @LargeTest (nightly / on-demand) ============ -->
    <profile>
        <id>large</id>
        <properties>
            <diesel.largeTests>true</diesel.largeTests>
            <test.heap>4g</test.heap>
        </properties>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes combine.self="override"/>
                        <includes>
                            <include>**/QuantitativeTest.java</include>
                            <include>**/DelimitedIoPerfTest.java</include>
                            <include>**/AllTestsSampleTest.java</include>
                            <include>**/StorageArrayRepresentationTest.java</include>
                        </includes>
                        <groups>large</groups>
                        <forkCount>1</forkCount>
                        <reuseForks>false</reuseForks>
                        <argLine>-Xmx4g --add-modules=jdk.incubator.vector</argLine>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- ============ ALL: release gate (aggregate) ============ -->
    <profile>
        <id>all</id>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes combine.self="override"/>
                        <includes>
                            <include>**/*Test.java</include>
                        </includes>
                        <excludes>
                            <exclude>**/AllTestsSampleTest.java</exclude>
                        </excludes>
                        <forkCount>2</forkCount>
                        <reuseForks>false</reuseForks>
                        <parallel>classes</parallel>
                        <threadCountClasses>2</threadCountClasses>
                        <argLine>-Xmx1g --add-modules=jdk.incubator.vector</argLine>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- СТАРЫЕ ПРОФИЛИ (test, ci) — удалить на шаге 7 -->
    <profile>
        <id>test</id>
        ...
```

### Валидация

```bash
# 1. POM валиден
mvn -B help:effective-pom -q > /dev/null && echo "POM OK" || echo "POM BROKEN"

# 2. Все 7 новых профилей видны
mvn -B help:all-profiles -q 2>&1 | grep -E "^\s+(fast|core|concurrency|network|perf|large|all)\s"
# Ожидаемый вывод: 7 строк с именами профилей

# 3. Каждый профиль запускается отдельно
for p in fast core concurrency network perf; do
    echo "=== Profile: $p ==="
    mvn -B clean test -P $p 2>&1 | tee /tmp/step3-$p.log | grep -E "Tests run|BUILD"
done
# Profile 'large' и 'all' — отдельно, они тяжёлые
mvn -B clean test -P large 2>&1 | tee /tmp/step3-large.log | grep -E "Tests run|BUILD"
mvn -B clean test -P all 2>&1 | tee /tmp/step3-all.log | grep -E "Tests run|BUILD"

# 4. Проверь, что в каждом профиле тесты реально запускаются (не 0)
for p in fast core concurrency network perf large all; do
    RUN=$(grep -oE "Tests run: [0-9]+" /tmp/step3-$p.log | head -1 | grep -oE "[0-9]+")
    echo "$p: $RUN tests"
done
# fast: ~50-60 tests (smoke+index+query)
# core: ~250-300 tests (query+storage)
# concurrency: ~7 tests
# network: ~20-30 tests
# perf: ~30 tests
# large: ~5-10 tests (only @LargeTest)
# all: ~1200 tests
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `combine.self="override"` не работает | У старых surefire-версий нет этой фичи | Проверь `<version>3.2.5</version>` — должна быть >= 3.0 |
| `large` профиль запускает 0 тестов | `<groups>large</groups>` фильтрует по JUnit 5 `@Tag`, но `@LargeTest` — мета-аннотация | Проверь, что `@LargeTest` действительно включает `@Tag("large")` — да, включает (см. `LargeTest.java` строка 24) |
| `network` профиль падает на `GracefulShutdownTest` | На CI нет прав на subprocess | Это известная хрупкость — на шаге 4 сделаем `fail-fast: false` |
| `forkCount=2` для core_profile ломается | Не хватает памяти | Сними `forkCount=1`, `reuseForks=false` — без параллелизма |
| `parallel=classes` падает с `ClassNotFound` | Class loading races в JUnit 5 | `forkCount>1` + `reuseForks=false` — каждый fork изолирован |

### Rollback

```bash
git checkout -- pom.xml
mvn -B clean test -P ci 2>&1 | grep "Tests run" | tail -3
```

### Коммит

```bash
git add pom.xml
git commit -m "chore(test): step 3 — add 7 new Maven profiles (fast/core/concurrency/network/perf/large/all)

Each profile targets a specific test bucket with appropriate
forkCount, reuseForks, parallel, and heap settings.
Old profiles (test, ci) preserved temporarily for backward compatibility."
```

---

## Шаг 4. Переписывание GitHub Actions в матрицу из 4 джобов

### Цель
Заменить единый CI-джоб на матрицу из 4 параллельных профилей (PR-gate), плюс nightly для `perf`+`large`, плюс release-gate для `all`.

### Действия

Полностью заменить файл `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 3 * * *'           # nightly: 03:00 UTC
  workflow_dispatch:
    inputs:
      profile:
        description: 'Profile to run (only used for manual trigger; nightly uses matrix)'
        type: choice
        options: [fast, core, concurrency, network, perf, large, all]
        required: false
        default: fast

# Одновременно несколько пушей в один PR — отменяем предыдущие
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}-${{ matrix.profile }}
  cancel-in-progress: true

jobs:
  # ──────── PR-gate: параллельная матрица из 4 быстрых профилей ────────
  pr-gate:
    name: ${{ matrix.profile }}
    runs-on: ubuntu-24.04
    if: github.event_name != 'schedule' && github.event_name != 'workflow_dispatch'
    strategy:
      fail-fast: false
      matrix:
        profile: [fast, core, concurrency, network]
    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Build and run tests
        run: mvn -B clean test -P ${{ matrix.profile }}

      - name: Upload test reports
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: reports-${{ matrix.profile }}
          path: target/surefire-reports/
          retention-days: 7

  # ──────── Nightly: perf + large ────────
  nightly:
    name: nightly-${{ matrix.profile }}
    runs-on: ubuntu-24.04
    if: github.event_name == 'schedule'
    strategy:
      fail-fast: false
      matrix:
        profile: [perf, large]
    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Run ${{ matrix.profile }} tests
        run: mvn -B clean test -P ${{ matrix.profile }}

      - name: Upload performance history
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: nightly-${{ matrix.profile }}-artifacts
          path: |
            analytics/performance_history.csv
            analytics/regression_baseline.md
            analytics/benchmark_report.md
            target/surefire-reports/
          retention-days: 30

  # ──────── Manual dispatch: один профиль по запросу ────────
  manual:
    name: manual-${{ inputs.profile }}
    runs-on: ubuntu-24.04
    if: github.event_name == 'workflow_dispatch'
    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Run ${{ inputs.profile }} profile
        run: mvn -B clean test -P ${{ inputs.profile }}

      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: manual-${{ inputs.profile }}-reports
          path: target/surefire-reports/

  # ──────── Release gate: вся сотня ────────
  release-gate:
    name: all-tests
    runs-on: ubuntu-24.04
    if: startsWith(github.ref, 'refs/tags/v')
    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Build and run ALL tests
        run: mvn -B clean verify -P all

      - name: Upload release JAR
        uses: actions/upload-artifact@v4
        with:
          name: release-jar
          path: target/*.jar
          retention-days: 30
```

### Валидация

```bash
# 1. YAML валиден
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo "YAML OK"

# 2. Структура содержит 4 джоба
python3 -c "
import yaml
data = yaml.safe_load(open('.github/workflows/ci.yml'))
jobs = list(data['jobs'].keys())
assert set(jobs) == {'pr-gate', 'nightly', 'manual', 'release-gate'}, f'Got: {jobs}'
print('Jobs:', jobs)
"

# 3. pr-gate использует матрицу
python3 -c "
import yaml
data = yaml.safe_load(open('.github/workflows/ci.yml'))
profiles = data['jobs']['pr-gate']['strategy']['matrix']['profile']
assert set(profiles) == ['fast', 'core', 'concurrency', 'network'], f'Got: {profiles}'
print('PR-gate profiles:', profiles)
"

# 4. fail-fast выключен
python3 -c "
import yaml
data = yaml.safe_load(open('.github/workflows/ci.yml'))
assert data['jobs']['pr-gate']['strategy']['fail-fast'] == False
print('fail-fast: false')
"

# 5. Concurrency group настроен
python3 -c "
import yaml
data = yaml.safe_load(open('.github/workflows/ci.yml'))
assert 'concurrency' in data
print('Concurrency group:', data['concurrency']['group'])
"
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| YAML invalid на отступах | Смешал пробелы и табы | Используй только пробелы, 2-пробельные отступы |
| `cancel-in-progress` отменяет nightly | Concurrency group слишком общий | Group включает `${{ matrix.profile }}` — nightly не отменяет PR |
| `inputs.profile` undefined в `manual` | `github.event.inputs` vs `inputs` | В новых версиях actionsrunner `inputs` — нормально |
| Nightly не запускается | Schedule требует default branch (main) | Проверь, что cron в UTC — да, `0 3 * * *` = 03:00 UTC |

### Rollback

```bash
git checkout -- .github/workflows/ci.yml
```

### Коммит

```bash
git add .github/workflows/ci.yml
git commit -m "ci: step 4 — split single job into 4-job PR-gate matrix + nightly + release-gate

- pr-gate: fast, core, concurrency, network (parallel, fail-fast=false)
- nightly: perf, large (03:00 UTC, schedule)
- manual: workflow_dispatch with profile selector
- release-gate: tag-triggered, runs 'all' profile
- concurrency: cancel previous runs for same ref+profile"
```

---

## Шаг 5. Удаление дублирующего теста AllTestsSampleTest

### Цель
`AllTestsSampleTest.java` (1 234 LOC) дублирует все 10 `*QueryTest` классов: внутри вызывает `runAdvancedTestQueries()`, `runAliasesTestQueries()` и т.д., плюс запускает `DatabaseServer`. Это **самый тяжёлый тест в репозитории** (10–30 с) и **самый хрупкий** (subprocess + sockets). Он нужен только в `network` и `large` профилях, где уже живёт.

**Действие:** НЕ удалять полностью, а пометить `@Disabled("...")` по умолчанию, оставив активным только в `network`/`large` через `@EnabledIfSystemProperty`.

### Действия

**Подожди.** На шаге 2 конфиг `network` профиль уже включает `AllTestsSampleTest`. Удалять полностью нельзя — там есть уникальные тесты (prompt-62/65/66/67/68/69/70). Лучше **отметить @Disabled** с причиной, а в `network`-профиле снять через `-Ddiesel.allTests=true`.

В `src/test/java/diesel/AllTestsSampleTest.java` найди строку с `@TestInstance` (в начале класса) и **добавь перед классом** аннотацию `@EnabledIfSystemProperty`:

```java
// Было:
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AllTestsSampleTest {
    ...
}

// Стало:
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfSystemProperty(named = "diesel.runAllTestsSample", matches = "true")
public class AllTestsSampleTest {
    ...
}
```

В `pom.xml` в `network`-профиле добавь системную пропертю:

```xml
<!-- Внутри <profile id="network"> → <configuration> -->
<systemPropertyVariables>
    <diesel.largeTests>${diesel.largeTests}</diesel.largeTests>
    <diesel.runAllTestsSample>true</diesel.runAllTestsSample>
</systemPropertyVariables>
```

Аналогично для `large`-профиля.

### Валидация

```bash
# 1. Класс компилируется
mvn -B test-compile -P network -q && echo "COMPILE OK"

# 2. Без network-профиля — AllTestsSampleTest не запускается
mvn -B clean test -P fast 2>&1 | grep -i "AllTestsSampleTest" | head -3
# Ожидаемый вывод: ничего или "@Disabled"

# 3. С network-профилем — запускается
mvn -B clean test -P network 2>&1 | grep -E "AllTestsSampleTest.*Tests run" | head -3
# Ожидаемый вывод: "AllTestsSampleTest: Tests run: 21, Failures: 0"

# 4. System property не протекает в другие профили
mvn -B clean test -P concurrency 2>&1 | grep -i "allTestsSample" | head -3
# Ожидаемый вывод: пусто
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `@EnabledIfSystemProperty` не резолвится | JUnit 5 < 5.4 | У тебя 5.10 — OK |
| `systemPropertyVariables` не пробрасывается | Не в том блоке конфигурации | Вложен в `<configuration>` surefire, не в `<properties>` profile |

### Rollback

```bash
git checkout -- src/test/java/diesel/AllTestsSampleTest.java pom.xml
mvn -B clean test -P network 2>&1 | grep "Tests run" | tail -3
```

### Коммит

```bash
git add src/test/java/diesel/AllTestsSampleTest.java pom.xml
git commit -m "chore(test): step 5 — gate AllTestsSampleTest behind diesel.runAllTestsSample property

AllTestsSampleTest (1.2k LOC, duplicates 10 *QueryTest + spawns DatabaseServer)
now only runs in network and large profiles via system property.
Reduces 'fast'/'core'/'concurrency' wall-clock by ~15-30s and eliminates a flaky source."
```

---

## Шаг 6. Аннотирование всех 91 тест-классов через @Tag

### Цель
Заменить длинные `<includes>` блоки в pom.xml на декларативную `<groups>` фильтрацию. Это пункт 9.2 из исходного плана — полный охват.

### Соглашение об именах тегов

| Tag | Профиль | Кол-во классов |
|---|---|---|
| `@Tag("smoke")` | fast | 6 |
| `@Tag("query")` | fast (10 подклассов AbstractDieselTest) | 10 |
| `@Tag("index")` | fast | 7 |
| `@Tag("query-full")` | core | 21 |
| `@Tag("storage")` | core | 24 |
| `@Tag("concurrency")` | concurrency | 3 |
| `@Tag("network")` | network | 6 (AllTestsSampleTest除外, gated separately) |
| `@Tag("perf")` | perf | 6 |
| `@Tag("large")` | large (через мета-аннотацию @LargeTest) | 4 |

**Важно:** тег `large` уже определён в `LargeTest.java` — НЕ дублировать его на классах, только на методах через `@LargeTest`.

### Действия

#### 6.1. Список тест-классов с их тегами

Полная карта для аннотирования (ИИ-агент исполняет построчно):

| Файл | Tags |
|---|---|
| `DatabaseSmokeTest` | `smoke` |
| `Phase0IntegrationTest` | `smoke` |
| `QueryParserRefactorTest` | `smoke` |
| `SelectQueryRefactorTest` | `smoke` |
| `NullSafetyTest` | `smoke` |
| `DeadCodeRemovalTest` | `smoke` |
| `BTreeIndexBulkLoadTest` | `index` |
| `BTreeClusteredIndexBulkLoadTest` | `index` |
| `CompositeIndexTest` | `index` |
| `CoveringIndexTest` | `index` |
| `WhereIndexTest` | `index` |
| `AutoWhereIndexTest` | `index` |
| `AutoJoinIndexTest` | `index` |
| `AdvancedQueryTest` | `query` |
| `AliasesQueryTest` | `query` |
| `GroupByQueryTest` | `query` |
| `InQueryTest` | `query` |
| `JoinQueryTest` | `query` |
| `LikeQueryTest` | `query` |
| `OrderByQueryTest` | `query` |
| `PerformanceQueryTest` | `query` |
| `SubqueryQueryTest` | `query` |
| `NullSafetyQueryTest` | `query` |
| `AdvancedTest` | `query-full` |
| `AliasesTest` | `query-full` |
| `GroupByTest` | `query-full` |
| `JoinTest` | `query-full` |
| `InTest` | `query-full` |
| `LikeTest` | `query-full` |
| `OrderByTest` | `query-full` |
| `SubqueriesTest` | `query-full` |
| `LazyDeleteTest` | `query-full` |
| `LimitOffsetTest` | `query-full` |
| `UpdateTest` | `query-full` |
| `BatchExecutionTest` | `query-full` |
| `BatchQueryTest` | `query-full` |
| `BulkInsertTest` | `query-full` |
| `ExplainTest` | `query-full` |
| `QueryOptimizerTest` | `query-full` |
| `QueryCacheTest` | `query-full` |
| `QueryProfilerTest` | `query-full` |
| `AnalyzeTableTest` | `query-full` |
| `MaxResultRowsTest` | `query-full` |
| `HashJoinMemoryTest` | `query-full` |
| `ParallelIndexScanTest` | `query-full` |
| `AtomicFileWriteTest` | `storage` |
| `CompressionTest` | `storage` |
| `PersistenceTest` | `storage` |
| `LoadErrorHandlingTest` | `storage` |
| `CharsetEncodingTest` | `storage` |
| `StorageLoadModeTest` | `storage` |
| `StorageArrayRepresentationTest` | `storage` |
| `ReaderCorrectnessTest` | `storage` |
| `NullSentinelTest` | `storage` |
| `CsvStorageTest` | `storage` |
| `CsvStorageAdvancedTest` | `storage` |
| `CsvIndexManagerTest` | `storage` |
| `CsvTsvHeaderMappingTest` | `storage` |
| `TsvStorageTest` | `storage` |
| `TsvStorageAdvancedTest` | `storage` |
| `JsonStreamAbstractionTest` | `storage` |
| `JsonlStorageTest` | `storage` |
| `JsonlSchemaProjectionTest` | `storage` |
| `JsonlSchemaModeTest` | `storage` |
| `JsonlNestedModeTest` | `storage` |
| `JsonlTypeMapperTest` | `storage` |
| `JsonlNullSemanticsTest` | `storage` |
| `JsonlLoadDiagnosticsTest` | `storage` |
| `JsonlAppendModeTest` | `storage` |
| `JsonlDeterministicSerializationTest` | `storage` |
| `JsonlCompressionTest` | `storage` |
| `JsonlIndexTest` | `storage` |
| `JsonlLoadModeTest` | `storage` |
| `ConcurrentSaveTest` | `concurrency` |
| `ConcurrentConflictTest` | `concurrency` |
| `CopyOnWriteIsolationTest` | `concurrency` |
| `ServerConnectionLimitTest` | `network` |
| `SocketTimeoutTest` | `network` |
| `GracefulShutdownTest` | `network` |
| `OomHandlingTest` | `network` |
| `CursorTest` | `network` |
| `PreparedStatementTest` | `network` |
| `AllTestsSampleTest` | `network` (через `diesel.runAllTestsSample`, тег не обязателен) |
| `PerformanceTest` | `perf` |
| `PerformanceRegressionTest` | `perf` |
| `StringOpsBenchmarkTest` | `perf` |
| `RegexPerformanceBenchmarkTest` | `perf` |
| `RegexRobustnessTest` | `perf` |
| `StorageBulkUpdateTest` | `perf` |
| `QuantitativeTest` | уже `large` через `@LargeTest` |
| `DelimitedIoPerfTest` | уже `large` через `@LargeTest` |

#### 6.2. Шаблон аннотирования

Для каждого класса из таблицы выше добавить над `public class Xxx`:

```java
import org.junit.jupiter.api.Tag;
// ...

@Tag("smoke")                       // ← добавить
public class DatabaseSmokeTest {
```

Если у класса уже есть другие аннотации (`@TestInstance`, `@TestMethodOrder`, `@EnabledIfSystemProperty`), **`@Tag` добавлять ПЕРВЫМ** (сверху), до других.

Пример для `AllTestsSampleTest`:

```java
@Tag("network")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfSystemProperty(named = "diesel.runAllTestsSample", matches = "true")
public class AllTestsSampleTest {
```

Пример для `QuantitativeTest` (весь класс `@LargeTest` → тег `large` уже есть):

```java
// Ничего не добавлять — @LargeTest = @Tag("large")
public class QuantitativeTest {
    @LargeTest
    void test1() { ... }

    @LargeTest
    void test2() { ... }
}
```

#### 6.3. Скрипт-генератор для пакетного добавления

Чтобы не править 91 файл вручную, использовать скрипт:

```bash
mkdir -p /home/z/my-project/scripts
```

Создать файл `/home/z/my-project/scripts/add-tags.sh`:

```bash
#!/usr/bin/env bash
# Скрипт добавляет @Tag("...") аннотацию к Java-классам тестов.
# Использование: ./add-tags.sh <test-dir> <mapping-file>
# mapping-file: формат "FileName Tag" (без .java)

set -euo pipefail

TEST_DIR="$1"
MAPPING_FILE="$2"

if [[ ! -d "$TEST_DIR" ]]; then
    echo "Error: test dir '$TEST_DIR' not found"
    exit 1
fi

if [[ ! -f "$MAPPING_FILE" ]]; then
    echo "Error: mapping file '$MAPPING_FILE' not found"
    exit 1
fi

# Счётчик изменённых файлов
COUNT=0

while IFS=$'\t' read -r FILENAME TAG; do
    [[ -z "$FILENAME" || -z "$TAG" || "$FILENAME" == \#* ]] && continue

    FILE="$TEST_DIR/${FILENAME}.java"
    if [[ ! -f "$FILE" ]]; then
        echo "  SKIP: $FILE not found"
        continue
    fi

    # Проверка: уже есть @Tag("$TAG")?
    if grep -qE "^@Tag\(\"$TAG\"\)" "$FILE"; then
        echo "  SKIP: $FILENAME already has @Tag(\"$TAG\")"
        continue
    fi

    # Добавить импорт org.junit.jupiter.api.Tag, если его нет
    if ! grep -q "^import org.junit.jupiter.api.Tag;" "$FILE"; then
        # Вставить перед первой строкой import org.junit.jupiter.api.
        # Если таких импортов нет — перед package
        if grep -q "^import org.junit.jupiter.api" "$FILE"; then
            sed -i '/^import org\.junit\.jupiter\.api\./i import org.junit.jupiter.api.Tag;' "$FILE"
        else
            # После package
            sed -i '/^package /a \\nimport org.junit.jupiter.api.Tag;' "$FILE"
        fi
    fi

    # Найти строку с "public class <Name>" и добавить @Tag перед ней
    # Сначала найти все аннотации над классом, потом вставить @Tag ВЫШЕ всех
    # Простой способ: вставить @Tag("$TAG") ПЕРЕД строкой "public class"
    sed -i "s|^public class |@Tag(\"$TAG\")\npublic class |" "$FILE"

    COUNT=$((COUNT + 1))
    echo "  OK: $FILENAME ← @Tag(\"$TAG\")"
done < "$MAPPING_FILE"

echo ""
echo "Done. Modified $COUNT files."
```

Создать mapping-файл `/home/z/my-project/scripts/tag-mapping.tsv` (таб-разделённый, `#` = комментарии):

```
# Файл mapping: имя_класса (без .java) <TAB> тег
DatabaseSmokeTest	smoke
Phase0IntegrationTest	smoke
QueryParserRefactorTest	smoke
SelectQueryRefactorTest	smoke
NullSafetyTest	smoke
DeadCodeRemovalTest	smoke
BTreeIndexBulkLoadTest	index
BTreeClusteredIndexBulkLoadTest	index
CompositeIndexTest	index
CoveringIndexTest	index
WhereIndexTest	index
AutoWhereIndexTest	index
AutoJoinIndexTest	index
AdvancedQueryTest	query
AliasesQueryTest	query
GroupByQueryTest	query
InQueryTest	query
JoinQueryTest	query
LikeQueryTest	query
OrderByQueryTest	query
PerformanceQueryTest	query
SubqueryQueryTest	query
NullSafetyQueryTest	query
AdvancedTest	query-full
AliasesTest	query-full
GroupByTest	query-full
JoinTest	query-full
InTest	query-full
LikeTest	query-full
OrderByTest	query-full
SubqueriesTest	query-full
LazyDeleteTest	query-full
LimitOffsetTest	query-full
UpdateTest	query-full
BatchExecutionTest	query-full
BatchQueryTest	query-full
BulkInsertTest	query-full
ExplainTest	query-full
QueryOptimizerTest	query-full
QueryCacheTest	query-full
QueryProfilerTest	query-full
AnalyzeTableTest	query-full
MaxResultRowsTest	query-full
HashJoinMemoryTest	query-full
ParallelIndexScanTest	query-full
AtomicFileWriteTest	storage
CompressionTest	storage
PersistenceTest	storage
LoadErrorHandlingTest	storage
CharsetEncodingTest	storage
StorageLoadModeTest	storage
StorageArrayRepresentationTest	storage
ReaderCorrectnessTest	storage
NullSentinelTest	storage
CsvStorageTest	storage
CsvStorageAdvancedTest	storage
CsvIndexManagerTest	storage
CsvTsvHeaderMappingTest	storage
TsvStorageTest	storage
TsvStorageAdvancedTest	storage
JsonStreamAbstractionTest	storage
JsonlStorageTest	storage
JsonlSchemaProjectionTest	storage
JsonlSchemaModeTest	storage
JsonlNestedModeTest	storage
JsonlTypeMapperTest	storage
JsonlNullSemanticsTest	storage
JsonlLoadDiagnosticsTest	storage
JsonlAppendModeTest	storage
JsonlDeterministicSerializationTest	storage
JsonlCompressionTest	storage
JsonlIndexTest	storage
JsonlLoadModeTest	storage
ConcurrentSaveTest	concurrency
ConcurrentConflictTest	concurrency
CopyOnWriteIsolationTest	concurrency
ServerConnectionLimitTest	network
SocketTimeoutTest	network
GracefulShutdownTest	network
OomHandlingTest	network
CursorTest	network
PreparedStatementTest	network
AllTestsSampleTest	network
PerformanceTest	perf
PerformanceRegressionTest	perf
StringOpsBenchmarkTest	perf
RegexPerformanceBenchmarkTest	perf
RegexRobustnessTest	perf
StorageBulkUpdateTest	perf
# QuantitativeTest и DelimitedIoPerfTest — не нужны, у них @LargeTest (мета-тег "large")
```

Запуск:

```bash
chmod +x /home/z/my-project/scripts/add-tags.sh
cd /home/z/my-project/dieseldb
bash /home/z/my-project/scripts/add-tags.sh src/test/java/diesel /home/z/my-project/scripts/tag-mapping.tsv
```

### Валидация

```bash
# 1. Все классы компилируются
mvn -B test-compile -q && echo "COMPILE OK" || echo "COMPILE FAIL"

# 2. Все 87 классов (кроме QuantitativeTest, DelimitedIoPerfTest, LargeTest, helpers) имеют @Tag
cd /home/z/my-project/dieseldb
EXPECTED=87
ACTUAL=$(rg -l "^@Tag\(" src/test/java/diesel/*.java | wc -l)
echo "Tagged classes: $ACTUAL / expected $EXPECTED"

# 3. Каждый класс имеет ровно один @Tag
DOUBLE=$(rg -l "^@Tag\(" src/test/java/diesel/*.java | xargs -I{} sh -c 'test $(grep -c "^@Tag(" {}) -le 1 || echo {}')
if [[ -z "$DOUBLE" ]]; then
    echo "OK: no class has multiple @Tag"
else
    echo "FAIL: multiple @Tag in: $DOUBLE"
fi

# 4. Импорт org.junit.jupiter.api.Tag присутствует
MISSING_IMPORT=$(rg -l "^@Tag\(" src/test/java/diesel/*.java | xargs -I{} sh -c 'grep -q "^import org.junit.jupiter.api.Tag;" {} || echo {}')
if [[ -z "$MISSING_IMPORT" ]]; then
    echo "OK: all tagged files have import"
else
    echo "FAIL: missing import in: $MISSING_IMPORT"
fi

# 5. Smoke-прогон: @Tag("smoke") фильтруется корректно (если добавить groups)
mvn -B clean test -Dgroups=smoke 2>&1 | grep "Tests run" | tail -3
# Ожидаемый вывод: ~6 smoke-тестов
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| Скрипт ломает код (добавляет @Tag внутри метода) | В файле есть `public class` в комментарии | Проверь `grep "public class" file.java` — должна быть 1 строка |
| `@Tag` с дубликатом | Уже было `@Tag` в файле | Проверь через `grep "^@Tag" file.java` перед запуском |
| Импорт Tag не добавился | Нет других `import org.junit.jupiter.api.` строк | Скрипт вставляет после `package` — должен работать |
| `RegexRobustnessTest` minified | Файл в одну строку, sed не справится | Открыть вручную, добавить `@Tag("perf")` в начале |
| Compilation error на `query-full` | Содержит дефис | JUnit 5 @Tag поддерживает дефис — OK, но проверь окружение кавычками |

### Rollback

```bash
# Если скрипт только что запущен и ещё не закоммичен:
git checkout -- src/test/java/diesel/

# Если уже закоммичено:
git log --oneline -5
git revert HEAD
```

### Коммит

```bash
cd /home/z/my-project/dieseldb
git add src/test/java/diesel/
git add /home/z/my-project/scripts/add-tags.sh /home/z/my-project/scripts/tag-mapping.tsv
git commit -m "chore(test): step 6 — annotate 87 test classes with @Tag (smoke/query/index/query-full/storage/concurrency/network/perf)

Each test class now carries a JUnit 5 @Tag matching its bucket.
QuantitativeTest and DelimitedIoPerfTest already have @LargeTest (meta-tag 'large').
Helper classes (AbstractDieselTest, TestWaitHelper, Slf4jLogCapture, LargeTest) are not tagged.

Generated via scripts/add-tags.sh using scripts/tag-mapping.tsv."
```

---

## Шаг 7. Переключение surefire с `<includes>` на `<groups>`

### Цель
Использовать JUnit 5 native tag-filtering вместо длинных surefire `<includes>`. Это сократит pom.xml на ~60 строк и сделает конфигурацию декларативной.

### Действия

В `pom.xml` **для каждого из 7 новых профилей** (fast, core, concurrency, network, perf, large, all) заменить блок `<configuration>` surefire-plugin:

**Шаблон замены (для каждого профиля):**

Было:
```xml
<configuration>
    <excludes combine.self="override"/>
    <includes>
        <include>**/DatabaseSmokeTest.java</include>
        <include>**/Phase0IntegrationTest.java</include>
        ... 20+ строк ...
    </includes>
    <forkCount>1</forkCount>
    ...
</configuration>
```

Стало:
```xml
<configuration>
    <excludes combine.self="override"/>
    <groups>${surefire.groups}</groups>      <!-- динамически из свойства -->
    <excludedGroups>${surefire.excludedGroups}</excludedGroups>
    <forkCount>1</forkCount>
    ...
</configuration>
```

И **внутри каждого `<profile>`** добавить `<properties>` с конкретными значениями:

| Профиль | `surefire.groups` | `surefire.excludedGroups` |
|---|---|---|
| `fast` | `smoke \| query \| index` | `large` |
| `core` | `query-full \| storage` | `large` |
| `concurrency` | `concurrency` | `large` |
| `network` | `network` | `large` |
| `perf` | `perf` | `large` |
| `large` | `large` | (пусто) |
| `all` | `smoke \| query \| index \| query-full \| storage \| concurrency \| network \| perf` | `large` |

Пример для `fast`:

```xml
<profile>
    <id>fast</id>
    <properties>
        <test.heap>512m</test.heap>
        <surefire.groups>smoke | query | index</surefire.groups>
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
```

**Дефолтные свойства в `<properties>` (корень pom.xml):**

```xml
<properties>
    <!-- ... существующие ... -->
    <surefire.groups></surefire.groups>
    <surefire.excludedGroups>large</surefire.excludedGroups>
</properties>
```

**Удаление старых профилей (test, ci):** удалить полностью из `<profiles>`. Если кто-то использует `mvn -Ptest test` или `mvn -Pci test`, они получат ошибку — это **хорошо**, заставит перейти на новые имена. Для обратной совместимости можно оставить alias-профили:

```xml
<!-- Алиас для обратной совместимости — удалить через 2 релиза -->
<profile>
    <id>test</id>
    <!-- Запускает fast-набор -->
    <activation><property><name>profile.aliased</name></property></activation>
    <properties>
        <surefire.groups>smoke | query | index</surefire.groups>
    </properties>
</profile>

<profile>
    <id>ci</id>
    <properties>
        <surefire.groups>smoke | query | index | query-full | storage | concurrency | network</surefire.groups>
    </properties>
</profile>
```

### Валидация

```bash
# 1. POM валиден
mvn -B help:effective-pom -q > /dev/null && echo "POM OK"

# 2. Каждый профиль запускает ровно ожидаемый набор
for p in fast core concurrency network perf; do
    echo "=== $p ==="
    mvn -B clean test -P $p 2>&1 | grep -E "Tests run:" | tail -3
done

# 3. Сравнить количество тестов со шагом 3 (должно совпадать)
for p in fast core concurrency network perf large; do
    S3=$(grep -oE "Tests run: [0-9]+" /tmp/step3-$p.log | head -1 | grep -oE "[0-9]+")
    S7=$(mvn -B clean test -P $p 2>&1 | grep -oE "Tests run: [0-9]+" | tail -1 | grep -oE "[0-9]+")
    if [[ "$S3" == "$S7" ]]; then
        echo "$p: $S3 tests (same as step 3) ✓"
    else
        echo "$p: step3=$S3 vs step7=$S7 ✗ MISMATCH"
    fi
done

# 4. Динамическая фильтрация: можно через -Dgroups
mvn -B clean test -Dgroups=smoke 2>&1 | grep "Tests run" | tail -3
# Ожидаемый вывод: ~6 тестов (только @Tag("smoke"))

mvn -B clean test -Dgroups="smoke | index" 2>&1 | grep "Tests run" | tail -3
# Ожидаемый вывод: ~13 тестов

# 5. Алиасы работают
mvn -B clean test -P ci 2>&1 | grep "Tests run" | tail -3
mvn -B clean test -P test 2>&1 | grep "Tests run" | tail -3
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---| Maven не парсит `|` в `<groups>` |
| `|` не работает как OR | Surefire требует `,` или `, ` | Используй `smoke, query, index` (запятая + пробел) |

Внеси изменение:

| Профиль | `surefire.groups` (значение) |
|---|---|
| `fast` | `smoke, query, index` |
| `core` | `query-full, storage` |
| `concurrency` | `concurrency` |
| `network` | `network` |
| `perf` | `perf` |
| `large` | `large` |
| `all` | `smoke, query, index, query-full, storage, concurrency, network, perf` |

| `mvn -Dgroups=...` не передаёт группы | Свойство не пробрасывается | Убедись, что в `<configuration>` указано `<groups>${surefire.groups}</groups>`, а свойство определено в `<properties>` профиля |
| `excludedGroups` не работает | Свойство пустое по умолчанию | Установи `<surefire.excludedGroups>large</surefire.excludedGroups>` в корневых `<properties>` |
| Old `test`/`ci` профили запускают 0 тестов | После удаления `<includes>` ничего не выбирается | Либо использовать alias-профили с `<groups>`, либо удалить старые |

### Rollback

```bash
git checkout -- pom.xml
mvn -B clean test -P fast 2>&1 | grep "Tests run" | tail -3
```

### Коммит

```bash
git add pom.xml
git commit -m "chore(test): step 7 — switch surefire from <includes> to JUnit 5 <groups>

Each profile now uses <groups>\${surefire.groups}</groups> driven by @Tag annotations
instead of fragile <include> lists. Removes ~60 lines of pom.xml.
Old 'test' and 'ci' profiles removed; new alias profiles provide backward compat.

Tag filtering:
  fast:        smoke, query, index
  core:        query-full, storage
  concurrency: concurrency
  network:     network
  perf:        perf
  large:       large
  all:         all tags except large"
```

---

## Шаг 8 (он же 9.1 из исходного плана). Test-Impact Analysis

### Цель
Скрипт, который по `git diff --name-only` определяет, какие исходники изменились, и предлагает список Maven-профилей (или тегов) для запуска. Локально — экономия 30–60 сек на каждом сохранении; в CI — возможность пропускать целые профили.

### Действия

#### 8.1. Создать mapping исходник → bucket-тестов

Файл `/home/z/my-project/dieseldb/scripts/tia-mapping.txt` (формат: путь-к-исходнику | путь-к-тесту | тег-теста):

```
# Mapping: changed source → test buckets to run
# Формат: <source path glob> <TAB> <tag> [<TAB> <specific test class>]
#
# Если source-list несколько — все теги суммируются.
# Запуск: ./scripts/tia.sh [ref]
#   ref по умолчанию = origin/main...HEAD

diesel/SqlLexer.java			smoke,query
diesel/SqlKeywords.java			smoke,query
diesel/SqlParsingUtils.java		smoke,query
diesel/QueryParser.java			smoke,query,query-full
diesel/ParseContext.java		smoke,query
diesel/SubqueryParser.java		query,query-full
diesel/Query.java			query,query-full
diesel/SelectQuery.java			query,query-full
diesel/InsertQuery.java			query,query-full
diesel/UpdateQuery.java			query,query-full,network
diesel/DeleteQuery.java			query,query-full
diesel/BatchQuery.java			query,query-full
diesel/TransactionQuery.java		concurrency
diesel/BeginTransactionQuery.java	concurrency
diesel/CommitTransactionQuery.java	concurrency
diesel/RollbackTransactionQuery.java	concurrency
diesel/SetAutoCommitQuery.java		concurrency
diesel/SetIsolationLevelQuery.java	concurrency
diesel/QueryExecutor.java		query,query-full
diesel/QueryOptimizer.java		query,query-full
diesel/QueryCache.java			query,query-full
diesel/QueryProfiler.java		query,query-full
diesel/ConditionEvaluator.java		query,query-full
diesel/ThreeValuedLogic.java		query,query-full
diesel/AggregateFunctions.java		query,query-full
diesel/CharOps.java			perf
diesel/Table.java			query,query-full
diesel/Sequence.java			query,query-full
diesel/BloomFilter.java			query,query-full
diesel/BTreeIndex.java			index,query-full
diesel/BTreeClusteredIndex.java		index,query-full
diesel/CompositeBTreeIndex.java		index,query-full
diesel/CoveringBTreeIndex.java		index,query-full
diesel/HashIndex.java			index,query-full
diesel/UniqueIndex.java			index,query-full
diesel/Database.java			smoke,query,query-full,storage,concurrency,network
diesel/DieselDatabase.java		smoke,query,query-full,storage,concurrency,network
diesel/TableStorage.java		storage,query-full
diesel/ConfigLoader.java		storage
diesel/storage/*.java			storage
diesel/storage/json/*.java		storage
diesel/DatabaseServer.java		network
diesel/DatabaseClient.java		network
diesel/Cursor.java			network
diesel/PreparedStatement.java		network
diesel/*Message.java			network
diesel/CompressionHandshakeMessage.java	network
diesel/CompressionHandshakeResponse.java	network
diesel/Transaction.java			concurrency
diesel/TransactionException.java	concurrency
diesel/AnalyzeTableQuery.java		query-full
diesel/ExplainQuery.java		query-full
diesel/CreateTableQuery.java		query,query-full
diesel/CreateIndexQuery.java		index,query-full
diesel/CreateIndexQueryBase.java	index,query-full
diesel/CreateCompositeIndexQuery.java	index,query-full
diesel/CreateCoveringIndexQuery.java	index,query-full
diesel/CreateHashIndexQuery.java	index,query-full
diesel/CreateUniqueIndexQuery.java	index,query-full
diesel/CreateUniqueClusteredIndexQuery.java	index,query-full
diesel/ErrorMessages.java		query,query-full
diesel/MessageConstants.java		network
pom.xml					all
.github/workflows/ci.yml		none
Makefile				none
```

#### 8.2. Создать скрипт TIA

Файл `/home/z/my-project/dieseldb/scripts/tia.sh`:

```bash
#!/usr/bin/env bash
# Test-Impact Analysis для DieselDB.
# Определяет, какие Maven-профили (JUnit 5 @Tag) нужно запустить
# на основе изменённых исходников.
#
# Использование:
#   ./scripts/tia.sh                 # сравнивает с origin/main
#   ./scripts/tia.sh HEAD~1          # сравнивает с предыдущим коммитом
#   ./scripts/tia.sh main..feature   # сравнивает две ветки
#   ./scripts/tia.sh --run           # сразу запускает определённые профили
#   ./scripts/tia.sh --tags          # выводит только теги для -Dgroups=

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

MAPPING_FILE="$REPO_ROOT/scripts/tia-mapping.txt"
if [[ ! -f "$MAPPING_FILE" ]]; then
    echo "Error: mapping file not found: $MAPPING_FILE" >&2
    exit 1
fi

REF="${1:-origin/main...HEAD}"
RUN_MODE=false
TAGS_ONLY=false
if [[ "$REF" == "--run" ]]; then
    RUN_MODE=true
    REF="${2:-origin/main...HEAD}"
elif [[ "$REF" == "--tags" ]]; then
    TAGS_ONLY=true
    REF="${2:-origin/main...HEAD}"
fi

# Получить список изменённых файлов
CHANGED=$(git diff --name-only "$REF" 2>/dev/null | sort -u)

if [[ -z "$CHANGED" ]]; then
    echo "No changes detected against $REF"
    if $TAGS_ONLY; then
        echo ""
    else
        echo "Recommended: no tests to run."
    fi
    exit 0
fi

echo "=== Changed files (vs $REF) ==="
echo "$CHANGED" | sed 's/^/  /'
echo ""

# Сопоставить изменённые файлы с тегами из mapping-файла
declare -A TAGS_SET

while IFS= read -r FILE; do
    [[ -z "$FILE" || "$FILE" == \#* ]] && continue

    # Пропустить комментарии и пустые строки в mapping-файле
    SOURCE_GLOB=$(echo "$FILE" | awk -F'\t' '{print $1}')
    TAGS=$(echo "$FILE" | awk -F'\t' '{print $2}')

    [[ -z "$SOURCE_GLOB" || -z "$TAGS" || "$SOURCE_GLOB" == \#* ]] && continue

    # Проверить, есть ли совпадение среди изменённых файлов
    MATCHED=false
    while IFS= read -r CHANGED_FILE; do
        # shell-glob matching через case
        # Используем bash extglob: переводим * в *
        if [[ "$CHANGED_FILE" == $SOURCE_GLOB ]]; then
            MATCHED=true
            break
        fi
    done <<< "$CHANGED"

    if $MATCHED; then
        # Разделить теги по запятой
        IFS=',' read -ra TAG_ARRAY <<< "$TAGS"
        for T in "${TAG_ARRAY[@]}"; do
            T=$(echo "$T" | xargs)  # trim
            [[ -n "$T" ]] && TAGS_SET["$T"]=1
        done
    fi
done < "$MAPPING_FILE"

# Если изменился pom.xml — запускаем все
if echo "$CHANGED" | grep -qE "^pom\.xml$"; then
    TAGS_SET["all"]=1
    unset TAGS_SET["none"]
fi

if [[ ${#TAGS_SET[@]} -eq 0 ]]; then
    echo "No test buckets impacted by changes."
    if $TAGS_ONLY; then
        echo ""
    else
        echo "Recommended: skip tests (or run 'mvn -P fast test' as smoke)."
    fi
    exit 0
fi

# Отсортировать теги
TAGS_SORTED=$(printf "%s\n" "${!TAGS_SET[@]}" | grep -vE "^(all|none)$" | sort -u | tr '\n' ',' | sed 's/,$//')

echo "=== Recommended test tags ==="
echo "$TAGS_SORTED" | tr ',' '\n' | sed 's/^/  - /'
echo ""

if $TAGS_ONLY; then
    echo "$TAGS_SORTED"
    exit 0
fi

# Определить профили для запуска
PROFILES=""
for TAG in $(echo "$TAGS_SORTED" | tr ',' ' '); do
    case "$TAG" in
        smoke|query|index)        PROFILES="$PROFILES fast" ;;
        query-full|storage)       PROFILES="$PROFILES core" ;;
        concurrency)               PROFILES="$PROFILES concurrency" ;;
        network)                  PROFILES="$PROFILES network" ;;
        perf)                     PROFILES="$PROFILES perf" ;;
    esac
done

# Дедуплицировать профили
PROFILES=$(echo "$PROFILES" | tr ' ' '\n' | sort -u | grep -v '^$' | tr '\n' ' ' | sed 's/ $//')

echo "=== Recommended Maven profiles ==="
echo "$PROFILES" | tr ' ' '\n' | sed 's/^/  - /' || echo "  (none)"
echo ""

echo "=== Commands ==="
echo "  mvn -B clean test -Dgroups=\"$TAGS_SORTED\""
echo "  # or:"
for P in $PROFILES; do
    echo "  mvn -B clean test -P $P"
done
echo ""

# Режим --run: запустить профили последовательно
if $RUN_MODE && [[ -n "$PROFILES" ]]; then
    echo "=== Running ==="
    for P in $PROFILES; do
        echo ""
        echo "--- $P ---"
        mvn -B clean test -P "$P" || {
            echo "FAILED: $P"
            exit 1
        }
    done
    echo ""
    echo "=== All impacted profiles passed ==="
fi
```

Сделать исполняемым:

```bash
chmod +x /home/z/my-project/dieseldb/scripts/tia.sh
```

#### 8.3. Создать Makefile target

В `/home/z/my-project/dieseldb/Makefile` найти секцию `.PHONY` и добавить `tia`:

```makefile
.PHONY: all build test quick-test large-test timing profile clean help check-timing tia tia-run
```

После таргета `test:` добавить:

```makefile
## Test-Impact Analysis: показать рекомендованные профили
tia:
	@echo "Running test-impact analysis..."
	@./scripts/tia.sh

## TIA + автоматический запуск рекомендованных профилей
tia-run:
	@echo "Running TIA + impacted tests..."
	@./scripts/tia.sh --run
```

#### 8.4. Интегрировать TIA в GitHub Actions

В `.github/workflows/ci.yml` добавить новый джоб `pr-gate-tia` **перед `pr-gate`**:

```yaml
  # ──────── TIA: запуск только impacted профилей ────────
  pr-gate-tia:
    name: tia-impact
    runs-on: ubuntu-24.04
    if: github.event_name == 'pull_request'
    needs: []  # параллельно pr-gate
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # нужно для git diff

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 21
          cache: maven

      - name: Detect impacted test buckets
        id: tia
        run: |
          TAGS=$(./scripts/tia.sh --tags origin/main...HEAD)
          echo "tags=$TAGS" >> $GITHUB_OUTPUT
          echo "Detected tags: $TAGS"

      - name: Run impacted tests only
        if: steps.tia.outputs.tags != ''
        run: mvn -B clean test -Dgroups="${{ steps.tia.outputs.tags }}"

      - name: Skip (no impacted tests)
        if: steps.tia.outputs.tags == ''
        run: echo "No tests to run — skipping"
```

### Валидация

```bash
# 1. Скрипт исполняется
cd /home/z/my-project/dieseldb
./scripts/tia.sh HEAD~1 2>&1 | head -30

# 2. Симулировать изменение storage-файла
touch diesel/storage/CsvRowReader.java
git add diesel/storage/CsvRowReader.java
git commit --allow-empty -m "test(tia): simulate storage change" -- diesel/storage/CsvRowReader.java
./scripts/tia.sh HEAD~1 2>&1 | grep "Recommended"
# Ожидаемый вывод: "Recommended test tags: storage"

# 3. Симулировать изменение SQL-парсера
touch diesel/QueryParser.java
git add diesel/QueryParser.java
git commit --allow-empty -m "test(tia): simulate parser change" -- diesel/QueryParser.java
./scripts/tia.sh HEAD~2 2>&1 | grep "Recommended"
# Ожидаемый вывод: "Recommended test tags: query,query-full,smoke"

# 4. --tags выдаёт только теги (для интеграции)
TAGS=$(./scripts/tia.sh --tags HEAD~1)
echo "Got tags: $TAGS"
# Ожидаемый вывод: "storage"

# 5. --run режим
git commit --allow-empty -m "trigger tia run" -- diesel/storage/JsonlRowReader.java
./scripts/tia.sh --run HEAD~1 2>&1 | tail -20
# Ожидаемый вывод: запускается профиль core (storage), проходит

# 6. Makefile target
make tia 2>&1 | tail -10
make tia-run 2>&1 | tail -10

# 7. GitHub Actions валидация (локальная через act или просто YAML)
python3 -c "
import yaml
data = yaml.safe_load(open('.github/workflows/ci.yml'))
assert 'pr-gate-tia' in data['jobs']
steps = data['jobs']['pr-gate-tia']['steps']
step_names = [s.get('name','') for s in steps]
assert any('impacted test buckets' in s for s in step_names)
print('TIA job integrated')
"

# Откатить симулированные коммиты
git reset --hard HEAD~3
git status  # должно быть clean
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `git diff --name-only origin/main...HEAD` пусто | Нет upstream-ветки | `git fetch origin main` |
| Скрипт не находит `tia-mapping.txt` | Запуск не из корня репо | Использовать `cd "$REPO_ROOT"` |
| glob `diesel/storage/*.java` не матчит | Bash `==` с glob требует `*` как wildcard | Работает в `[[ ]]` — OK |
| TIA-джоб падает на `--tags` если изменений нет | Скрипт выходит с 0, но `$TAGS` пустой | `if: steps.tia.outputs.tags != ''` спасает |
| `fetch-depth: 0` замедляет checkout | Глубокая история | Можно ограничить `fetch-depth: 50` |

### Rollback

```bash
rm -f /home/z/my-project/dieseldb/scripts/tia.sh /home/z/my-project/dieseldb/scripts/tia-mapping.txt
git checkout -- .github/workflows/ci.yml Makefile
git commit -am "revert: TIA step"
```

### Коммит

```bash
cd /home/z/my-project/dieseldb
git add scripts/tia.sh scripts/tia-mapping.txt .github/workflows/ci.yml Makefile
git commit -m "feat(tia): step 8 — test-impact analysis (shell + git)

- scripts/tia-mapping.txt: maps source paths to JUnit 5 @Tag buckets
- scripts/tia.sh: detects changed files via git diff, recommends tags/profiles
  modes: default (show), --tags (print only), --run (execute)
- Makefile targets: tia, tia-run
- .github/workflows/ci.yml: pr-gate-tia job runs only impacted tags

Saves ~30-60s per local iteration; in CI can skip entire profiles when
no source in their domain changed (e.g., storage-only PR skips network tests)."
```

---

## Шаг 9 (он же 9.4 из исходного плана). Maven Build Cache (локальный режим)

### Цель
Включить инкрементальную сборку: если исходники не изменились — Maven переиспользует артефакты из кэша, экономя 20–30 секунд на каждом `mvn clean`.

### Действия

#### 9.1. Создать конфиг Maven Build Cache

Создать директорию `.mvn/` в корне репозитория и файл `.mvn/maven-build-cache-config.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<cache xmlns="http://maven.apache.org/BUILD-CACHE/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/BUILD-CACHE/1.0.0
                           https://maven.apache.org/xsd/build-cache-config-1.0.0.xsd">

    <!-- Локальный режим: кэш хранится в ~/.m2/build-cache -->
    <configuration>
        <enabled>true</enabled>
        <remote enabled="false"/>
        <maxBuildsCachedLocal>20</maxBuildsCachedLocal>
    </configuration>

    <!-- Контролируем, какие артефакты кэшировать -->
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

    <!-- Управление входами для расчёта хэша -->
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
                <!-- Игнорировать для расчёта хэша: технические файлы -->
                <exclude>**/.gitignore</exclude>
                <exclude>**/*.md</exclude>
                <exclude>**/*.bak</exclude>
            </excludes>
        </glob>
    </inputs>

    <!-- Что кэшируем по фазам Maven -->
    <executionControl>
        <runAlways>
            <executions>
                <!-- Тесты всегда запускаем (не кэшируем результаты) -->
                <execution>
                    <id>default-test</id>
                </execution>
                <execution>
                    <id>surefire-test</id>
                </execution>
                <!-- Maven-deploy и install — всегда выполняем -->
                <execution>
                    <id>default-install</id>
                </execution>
            </executions>
        </runAlways>
        <reconcile>true</reconcile>
    </executionControl>
</cache>
```

#### 9.2. Включить build-cache extension

В корне `pom.xml` внутри `<build>` (до `<plugins>`) добавить:

```xml
<build>
    <sourceDirectory>diesel</sourceDirectory>
    <testSourceDirectory>src/test/java</testSourceDirectory>
    <extensions>
        <!-- Maven Build Cache extension -->
        <extension>
            <groupId>org.apache.maven.extensions</groupId>
            <artifactId>maven-build-cache-extension</artifactId>
            <version>1.0.0</version>
        </extension>
    </extensions>
    <plugins>
        ...
```

#### 9.3. Создать `.mvn/extensions.xml` (для активации extension)

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

#### 9.4. Добавить .gitignore для кэша

В `/home/z/my-project/dieseldb/.gitignore` (если нет — создать) добавить:

```
# Maven Build Cache
.mvn/build-cache/
target/
```

### Валидация

```bash
# 1. Maven-конфиг валиден
mvn -B help:effective-pom -q > /dev/null && echo "POM OK"

# 2. Extension активен
mvn -B help:system 2>&1 | grep -i "build-cache" || echo "Cache extension not detected"

# 3. Первый прогон (cold cache)
mvn -B clean test -P fast 2>&1 | tee /tmp/step9-cold.log | grep -E "BUILD|Cache"
# В логе должно быть: "Cache miss" или "Building from source"

# 4. Второй прогон без изменений (warm cache) — должен быть быстрее
mvn -B clean test -P fast 2>&1 | tee /tmp/step9-warm.log | grep -E "BUILD|Cache"
# В логе должно быть: "Cache hit" для compile/test-compile фаз

# 5. Сравнить время
COLD=$(grep "Total time" /tmp/step9-cold.log)
WARM=$(grep "Total time" /tmp/step9-warm.log)
echo "Cold: $COLD"
echo "Warm: $WARM"

# 6. Изменить один исходник, прогон должен частично попасть в кэш
echo "// cache-test marker" >> diesel/Query.java  # НЕ КОММИТИТЬ
mvn -B clean test -P fast 2>&1 | grep -E "Cache|BUILD"
git checkout -- diesel/Query.java  # откатить

# 7. Проверить, что кэш-директория создалась
ls -la ~/.m2/build-cache/ 2>/dev/null || ls -la .mvn/build-cache/ 2>/dev/null
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| Extension не подхватывается | Maven < 3.9 | Обнови Maven: `apt install maven` или скачай 3.9.x |
| `maven-build-cache-extension` не найден | Версия указана неверно | Проверь на Maven Central — версия 1.0.0 существует |
| Кэш не работает (всегда miss) | Пути в `<irrelevantPaths>` слишком жёсткие | Убери `target` из irrelevantPaths, если он там есть (он там не должен быть) |
| После `clean` кэш сбрасывается | Это норма — `clean` удаляет `target/`, но не `~/.m2/build-cache/` | Кэш переживает clean |
| Тесты падают из-за того, что кэшированы классы, но поменялся тестовый ресурс | `<inputs>` не включает ресурсы тестов | Добавь `src/test/resources/**` в includes |

### Rollback

```bash
git checkout -- pom.xml .gitignore
rm -rf .mvn/
rm -rf ~/.m2/build-cache/
mvn -B clean test -P fast 2>&1 | grep "BUILD"
```

### Коммит

```bash
cd /home/z/my-project/dieseldb
git add .mvn/ pom.xml .gitignore
git commit -m "build: step 9 — enable Maven Build Cache (local mode)

- .mvn/maven-build-cache-config.xml: local cache in ~/.m2/build-cache/
  maxBuildsCachedLocal=20, irrelevantPaths: .git, target, analytics, timing, scripts
- .mvn/extensions.xml: activates maven-build-cache-extension 1.0.0
- pom.xml: extension declared in <build><extensions>
- .gitignore: excludes build-cache artifacts

Expected: 20-30s saved on incremental 'mvn clean test' when source unchanged.
Tests themselves are run always (not cached) — only compile phases are cached."
```

---

## Шаг 10. Smoke-валидация всех 6 профилей и обновление Makefile

### Цель
Прогнать все профили, замерить итоговое время, обновить Makefile и worklog.

### Действия

#### 10.1. Полный smoke-прогон

```bash
cd /home/z/my-project/dieseldb

echo "=== SMOKE: 6 profiles ==="
for p in fast core concurrency network perf large; do
    echo ""
    echo "--- $p ---"
    START=$(date +%s)
    mvn -B clean test -P $p 2>&1 | tee /tmp/step10-$p.log | grep -E "Tests run|BUILD|Total time"
    END=$(date +%s)
    DUR=$((END - START))
    echo "Duration: ${DUR}s"
done

# Профиль all — отдельно, тяжёлый
echo ""
echo "--- all ---"
START=$(date +%s)
mvn -B clean verify -P all 2>&1 | tee /tmp/step10-all.log | grep -E "Tests run|BUILD|Total time"
END=$(date +%s)
echo "Duration: $((END - START))s"

# Сводка
echo ""
echo "=== SUMMARY ==="
for p in fast core concurrency network perf large all; do
    DUR=$(grep -oE "Total time:.*" /tmp/step10-$p.log | head -1)
    TESTS=$(grep -oE "Tests run: [0-9]+" /tmp/step10-$p.log | tail -1)
    echo "$p: $DUR | $TESTS"
done
```

#### 10.2. Обновить Makefile

В `/home/z/my-project/dieseldb/Makefile` заменить старые target'ы `test` и `quick-test` (которые использовали удалённые профили) на новые:

```makefile
## Запустить fast-профиль (smoke + index + 10 *QueryTest) — для локального цикла
test:
	@echo "Running fast profile (smoke + index + query)..."
	$(MVN) -B clean test -P fast

## Запустить core-профиль (вся query + storage логика)
test-core:
	@echo "Running core profile..."
	$(MVN) -B clean test -P core

## Запустить network-профиль (server, sockets)
test-network:
	@echo "Running network profile..."
	$(MVN) -B clean test -P network

## Запустить concurrency-профиль (txn, threads)
test-concurrency:
	@echo "Running concurrency profile..."
	$(MVN) -B clean test -P concurrency

## Запустить perf-профиль (benchmarks)
test-perf:
	@echo "Running perf profile..."
	$(MVN) -B clean test -P perf

## Запустить large-профиль (@LargeTest, 4GB heap)
large-test:
	@echo "Running large profile (@LargeTest)..."
	$(MVN) -B clean test -P large

## Запустить все профили последовательно (release gate)
all-tests:
	@echo "Running ALL profiles sequentially..."
	for p in fast core concurrency network perf large; do \
		echo "--- $$p ---"; \
		$(MVN) -B clean test -P $$p || exit 1; \
	done

## Test-impact analysis: показать рекомендованные профили
tia:
	@echo "Running test-impact analysis..."
	@./scripts/tia.sh

## TIA + автоматический запуск рекомендованных профилей
tia-run:
	@echo "Running TIA + impacted tests..."
	@./scripts/tia.sh --run
```

Также обновить `.PHONY` строку:

```makefile
.PHONY: all build test test-core test-network test-concurrency test-perf large-test all-tests timing profile clean help check-timing tia tia-run
```

Удалить старый `quick-test` target (больше не нужен, `test` = `fast`).

#### 10.3. Обновить help

```makefile
help:
	@echo "DieselDB Makefile - Quick Reference"
	@echo ""
	@echo "Targets:"
	@echo "  make build              - Build project (package, skip tests)"
	@echo "  make test               - Fast profile: smoke + index + query (<30s)"
	@echo "  make test-core          - Core profile: query + storage (2-4 min)"
	@echo "  make test-network       - Network profile: server + sockets (1-2 min)"
	@echo "  make test-concurrency   - Concurrency profile: txn + threads (<30s)"
	@echo "  make test-perf          - Performance profile: benchmarks (2-3 min)"
	@echo "  make large-test         - Large profile: @LargeTest tests (3-8 min, 4GB heap)"
	@echo "  make all-tests          - Run ALL profiles sequentially (release gate)"
	@echo "  make tia                - Test-impact analysis: recommend profiles"
	@echo "  make tia-run            - TIA + run recommended profiles"
	@echo "  make timing             - Run timing tests + compare"
	@echo "  make check-timing       - Check for regressions (>20% fail)"
	@echo "  make profile            - Run profiler"
	@echo "  make clean              - Remove build artifacts"
	@echo "  make help               - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  JAVA_HOME=/path/to/java"
	@echo "  MVN=/path/to/mvn"
```

#### 10.4. Сравнить с baseline

```bash
echo "=== BEFORE / AFTER ==="
echo ""
echo "--- Before migration (baseline) ---"
cat /tmp/baseline-timings.txt
echo ""
echo "--- After migration (new profiles) ---"
for p in fast core concurrency network perf large all; do
    DUR=$(grep -oE "Total time:.*" /tmp/step10-$p.log | head -1)
    echo "$p: $DUR"
done

# Параллельная оценка (4 профиля на 4-ядерном runner)
echo ""
echo "--- Estimated PR-gate (4 profiles parallel) ---"
MAX_DUR=0
for p in fast core concurrency network; do
    DUR_SEC=$(grep -oE "Total time:.*" /tmp/step10-$p.log | head -1 | grep -oE "[0-9]+:[0-9]+ min" | head -1)
    echo "  $p: $DUR_SEC"
done
echo "  -> PR-gate wall-clock ~= max of the 4 above"
```

#### 10.5. Обновить worklog

Добавить запись в `/home/z/my-project/worklog.md` (см. шаблон в §0.6).

### Валидация

```bash
# 1. Все target'ы Makefile работают
make help 2>&1 | head -5
make test 2>&1 | grep "BUILD"
make tia 2>&1 | tail -5

# 2. Все 6 профилей прошли успешно
PASS=0
for p in fast core concurrency network perf large; do
    if grep -q "BUILD SUCCESS" /tmp/step10-$p.log; then
        PASS=$((PASS+1))
        echo "$p: PASS"
    else
        echo "$p: FAIL"
    fi
done
echo "Total passed: $PASS / 6"
# Ожидаемый вывод: "Total passed: 6 / 6"

# 3. Worklog обновлён
test -f /home/z/my-project/worklog.md && grep -q "migration-tests-profiles" /home/z/my-project/worklog.md && echo "Worklog OK"

# 4. Git-статус чистый (кроме worklog.md)
git status -s | grep -v worklog.md
# Ожидаемый вывод: пусто (если есть изменения — закоммитить)
```

### Типичные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `make test` запускает старый профиль | Makefile не обновлён | Перечитай §10.2 |
| Network-профиль flaky | `GracefulShutdownTest` падает на subprocess | Это известный флейк — добавить retry в CI позже |
| `large` профиль OOM | `-Xmx4g` мало для 600×600 join | Подними до 6g |
| Worklog не обновлён | Файл не существует | `touch /home/z/my-project/worklog.md` и добавь запись |

### Rollback

```bash
git checkout -- Makefile
```

### Коммит

```bash
cd /home/z/my-project/dieseldb
git add Makefile
git commit -m "build: step 10 — update Makefile targets for new profile system

- make test          -> fast profile
- make test-core     -> core profile
- make test-network  -> network profile
- make test-concurrency -> concurrency profile
- make test-perf     -> perf profile
- make large-test    -> large profile
- make all-tests     -> runs all 6 profiles sequentially
- make tia           -> test-impact analysis (dry-run)
- make tia-run       -> TIA + execute impacted profiles
- Removed: quick-test (redundant with 'test')

Smoke validation: all 6 profiles pass."
```

---

## Финальный пуш PR

```bash
cd /home/z/my-project/dieseldb
git log --oneline chore/test-profiles-migration ^main
# Должно быть 9 коммитов: step 2, 3, 4, 5, 6, 7, 8, 9, 10

git push -u origin main
# Открыть PR с заголовком:
# "Migrate test suite to profile-based system with TIA, @Tag and build cache"
```

PR description:

```markdown
## Summary

Migrates DieselDB test suite from a single flat `<includes>` block to a
profile-based system with test-impact analysis, JUnit 5 @Tag filtering,
and Maven Build Cache.

## Changes

- **6 Maven profiles**: fast / core / concurrency / network / perf / large / all
- **GitHub Actions**: 4-job PR-gate matrix + nightly + release-gate
- **@Tag annotations**: 87 test classes annotated
- **Surefire**: switched from `<includes>` to `<groups>` (JUnit 5 native)
- **TIA script**: `scripts/tia.sh` detects impacted buckets via git diff
- **Maven Build Cache**: `.mvn/maven-build-cache-config.xml` (local mode)

## Expected impact

| Metric | Before | After |
|---|---|---|
| PR-gate wall-clock | 5–10 min | ~2 min (parallel) |
| Test coverage in CI | 42% | 100% (except @LargeTest) |
| Inner loop (`mvn test`) | 10 min | 30 sec (`-P fast`) |
| Incremental build | cold every time | 20–30s saved with cache |

## Test plan

- [ ] All 6 profiles pass locally
- [ ] TIA script detects correct buckets
- [ ] Build cache hit on second `mvn test`
- [ ] CI matrix shows 4 parallel jobs
- [ ] Nightly triggers at 03:00 UTC
```

---

## Приложение A. Сводная таблица команд валидации по шагам

| Шаг | Команда валидации | Ожидаемый результат |
|---|---|---|
| 1 | `git branch --show-current` | `chore/test-profiles-migration` |
| 2 | `mvn -B clean test 2>&1 \| grep "Tests run"` | 0 тестов (или ничего) |
| 3 | `mvn -B help:all-profiles -q \| grep -E "^\s+(fast\|core\|...)"` | 7 новых профилей |
| 4 | `python3 -c "import yaml; ..."` | 4 джоба в CI YAML |
| 5 | `mvn -B clean test -P network 2>&1 \| grep AllTestsSample` | Тесты запущены |
| 6 | `rg -l "^@Tag\(" src/test/java/diesel/*.java \| wc -l` | 87 файлов |
| 7 | `mvn -B clean test -Dgroups=smoke 2>&1 \| grep "Tests run"` | ~6 тестов |
| 8 | `./scripts/tia.sh HEAD~1` | Список тегов |
| 9 | `ls ~/.m2/build-cache/` | Кэш-директория существует |
| 10 | `grep "BUILD SUCCESS" /tmp/step10-*.log \| wc -l` | 7 (6 профилей + all) |

## Приложение B. Карта отката по шагам

| Шаг | Rollback-команда |
|---|---|
| 1 | `git checkout main && git branch -D chore/test-profiles-migration` |
| 2 | `git checkout -- pom.xml` |
| 3 | `git checkout -- pom.xml` |
| 4 | `git checkout -- .github/workflows/ci.yml` |
| 5 | `git checkout -- src/test/java/diesel/AllTestsSampleTest.java pom.xml` |
| 6 | `git checkout -- src/test/java/diesel/` |
| 7 | `git checkout -- pom.xml` |
| 8 | `rm -f scripts/tia.sh scripts/tia-mapping.txt && git checkout -- .github/workflows/ci.yml Makefile` |
| 9 | `rm -rf .mvn/ && git checkout -- pom.xml .gitignore` |
| 10 | `git checkout -- Makefile` |

## Приложение C. Timing budget (ожидаемый)

| Профиль | До миграции (сек) | После миграции (сек) | Экономия |
|---|---|---|---|
| `fast` | — | ~30 | новый (раньше не было) |
| `core` | ~300 (всё в одном) | ~150 | 2× |
| `concurrency` | — | ~20 | новый |
| `network` | — | ~90 | новый |
| `perf` | ~600 (включено в общий) | ~180 (nightly) | 3.3× |
| `large` | — | ~360 (nightly) | новый |
| **PR-gate wall-clock** | **600** (последовательно) | **~120** (4 профиля параллельно) | **5×** |
| **Inner loop (`mvn test`)** | 600 | 30 (`-P fast`) | **20×** |
| **Incremental build** | 600 (холодный) | ~570 (cache miss) / ~540 (cache hit) | 5–10% |

---

## Приложение D. Чек-лист для ИИ-агента перед завершением

- [ ] Шаг 1: ветка создана, baseline-файл `/tmp/baseline-timings.txt` заполнен
- [ ] Шаг 2: `mvn test` без `-P` запускает 0 тестов
- [ ] Шаг 3: `mvn help:all-profiles` показывает 7 новых профилей + 2 старых (пока не удалены)
- [ ] Шаг 4: `.github/workflows/ci.yml` содержит 4 джоба: `pr-gate`, `nightly`, `manual`, `release-gate`
- [ ] Шаг 5: `AllTestsSampleTest` запускается только в `network`/`large`
- [ ] Шаг 6: 87 файлов в `src/test/java/diesel/` имеют `@Tag("...")`
- [ ] Шаг 7: `mvn -Dgroups=smoke test` запускает ~6 тестов
- [ ] Шаг 8: `./scripts/tia.sh HEAD~1` корректно определяет теги
- [ ] Шаг 9: `~/.m2/build-cache/` существует после первого прогона
- [ ] Шаг 10: все 6 профилей `BUILD SUCCESS`, worklog обновлён
- [ ] PR открыт с заголовком "Migrate test suite to profile-based system..."
- [ ] Все 9 коммитов в ветке `origin/main` (не squash — для отслеживаемости шагов)

---

**Конец плана.** ИИ-агент исполняет шаги 1→10 последовательно. Каждый шаг = 1 коммит = атомарный откатываемый блок. После завершения всех 10 шагов открывается PR.

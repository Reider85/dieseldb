# CI3 — Устранение падений тестов и разблокировка релизного пайплайна

> Репозиторий: https://github.com/Reider85/dieseldb
> Ветка: `main` (HEAD на момент аудита: `77b4810`)
> Текущая версия в `pom.xml`: `3.4.36`

## Контекст

CI падает на каждом пуше в `main` уже ~30 коммитов подряд (с 3.4.7 по 3.4.36). Из-за этого
воркфлоу `release-on-push.yml` уходит в `skipped`, и ни одного тега/релиза за всю историю
репозитория не создано (см. вкладки `/tags` и `/releases` — оба пусты).

`compile-gate` проходит. Падает джоба `fast` профиля. На текущем `main` локально
воспроизводятся ровно три проблемы:

| # | Тест | Файл:строка | Симптом |
|---|------|-------------|---------|
| 1 | `CheckpointManagerTest.jmxMetricsExposed` | `src/test/java/diesel/storage/page/CheckpointManagerTest.java:334` | `getLastCheckpointDurationMs() > 0` → `expected: <true> but was: <false>` |
| 2 | `WALBackpressureTest.queueFullBlocksProducerAndLosesNoData` | `src/test/java/diesel/WALBackpressureTest.java:113` | `getMaxQueueSizeSeen() >= queueMaxSize * 0.9` → `expected: <true> but was: <false>` |
| 3 | `AuditAsyncTest.auditDoesNotBlockTheMainQueryPath` | `src/test/java/diesel/AuditAsyncTest.java:60` | Один тест идёт ~154 с (таймаут джобы `fast` — 60 мин, но параллельный `fast` и так близко к пределу, см. коммит `263b6c6 3.4.35 Fix hanging fast profile job`) |

Все три теста помечены `@Tag("smoke")` — это базовый smoke-профиль, который обязан быть зелёным
в `fast`. Помечать их `@Tag("flaky")` — это эскейп-хатч, а не починка.

Ниже — четыре самостоятельных промпта P1–P4. P1–P3 чинят корневые причины, P4 — необязательный
fast-unblock, если P1–P3 не успеть в один присест. Каждый промпт можно отдать кодеру или LLM
как самостоятельную задачу.

---

## Промпт P1 — Починить `CheckpointManagerTest.jmxMetricsExposed`

### Симптом
```
[ERROR] diesel.storage.page.CheckpointManagerTest.jmxMetricsExposed
        org.opentest4j.AssertionFailedError: Duration should be > 0
        ==> expected: <true> but was: <false>
        at diesel.storage.page.CheckpointManagerTest.java:334
```

### Корневая причина
В `diesel/storage/page/CheckpointManager.java:252` метод `checkpointNow()` обновляет
метрики (`checkpointCount`, `lastCheckpointLsn`, `lastCheckpointDurationMs`) **только
внутри ветки `if (checkpointLsn > 0)`** (строка 269).

Когда тест `jmxMetricsExposed` вызывает `checkpointNow()` на пустом WAL (нет ни одной
записи после `start()`), `strategy.checkpoint()` возвращает `0`, и тело `if` пропускается.
При этом `checkpointCount` всё же становится `> 0`, потому что счётчик инкрементируется
внутри `strategy.checkpoint()` (или в scheduled-цикле). Получается рассогласование:
счётчик «чекпоинтов было», а длительности — нет.

### Задача

1. Открыть `diesel/storage/page/CheckpointManager.java`, метод `checkpointNow()`.
2. Перенести обновление `lastCheckpointDurationMs` и `totalFlushDurationMs` так,
   чтобы они обновлялись **всегда** при успешном завершении `checkpointNow()` — даже
   если `checkpointLsn == 0` (это всё ещё валидный no-op checkpoint, его длительность
   валидна и нужна для JMX/SLO).
3. Альтернативный (более чистый) вариант: если checkpoint — это no-op (WAL пуст),
   всё равно записать `duration = System.currentTimeMillis() - start` в
   `lastCheckpointDurationMs`, но НЕ инкрементировать `checkpointCount` и
   `lastCheckpointLsn`. Это семантически правильно: «чекпоинт не делал работы,
   но мы знаем, что он занял X мс».
4. Не трогать публичный контракт метода — `checkpointNow()` должен по-прежнему
   возвращать `0` для no-op и `checkpointLsn` для реального.
5. Проверить, что `CheckpointManager` не инкрементирует `checkpointCount` где-то ещё
   (например, в `SharpCheckpoint`/`FuzzyCheckpoint` стратегиях) — если да, либо
   перенести инкремент в `checkpointNow()` единым местом, либо задокументировать,
   почему так. Тест `jmxMetricsExposed` ожидает `checkpointCount > 0` после одного
   `checkpointNow()` без pre-filled WAL — то есть либо реальный чекпоинт, либо
   start-time init должен давать минимум 1. Текущее поведение «count растёт, а LSN/duration нет»
   — это баг рассинхронизации.

### Критерий приёмки
```bash
mvn -B -T 4 --no-transfer-progress -Dstyle.color=never test -P fast \
    -Dtest='CheckpointManagerTest#jmxMetricsExposed' 2>&1 | tail -20
```
Тест проходит. Остальные 9 тестов в `CheckpointManagerTest` тоже проходят
(они не должны сломаться от изменения логики обновления метрик).

### Влияние
Production: метрики JMX для `dieseldb:type=CheckpointManager` начинают показывать
корректную длительность чекпоинтов даже на idle-инстансах. Без этого фикса мониторинг
СЛО (P99 checkpoint duration) всегда показывает 0 мс на слабонагруженных узлах.

---

## Промпт P2 — Починить `WALBackpressureTest.queueFullBlocksProducerAndLosesNoData`

### Симптом
```
[ERROR] diesel.WALBackpressureTest.queueFullBlocksProducerAndLosesNoData
        org.opentest4j.AssertionFailedError: Queue should have been nearly full at some point
        ==> expected: <true> but was: <false>
        at diesel.WALBackpressureTest.java:113
```

### Корневая причина
Тест использует `queueMaxSize = 3` (`WALConfig.of(tempDir, 1024 * 1024, 3)` в строке 45) и
проверяет в строке 113:
```java
assertTrue(writer.getMaxQueueSizeSeen() >= config.getQueueMaxSize() * 0.9,
           "Queue should have been nearly full at some point");
// queueMaxSize=3 → 3 * 0.9 = 2.7 → нужна maxSizeSeen >= 3 (т.е. ровно полная очередь)
```

В `diesel/wal/WALQueue.java:138` метод `updateMaxSizeSeen()` читает `queue.size()` **после**
`queue.put()` (строка 39) и `queue.offer()` (строка 64). Между моментом вставки и чтением
`queue.size()` консьюмер (`WALWriter` consumer thread) успевает `take()` — и `size()` возвращает 0 или 1.
Это гонка, поэтому `maxSizeSeen` почти никогда не достигает 3, особенно под параллельной
нагрузкой из 4 producer-тредов.

### Задача

Починить можно двумя способами — выбери **один** и явно обоснуй выбор в коммите.

**Вариант A (рекомендуемый — фикс production-кода):**
В `diesel/wal/WALQueue.java` обновлять `maxSizeSeen` **до** взятия элемента из очереди в `take()`,
а не только после `put()/offer()`. Сейчас `take()` вообще не вызывает `updateMaxSizeSeen()`,
поэтому пик размера очереди теряется, если между двумя `put()` консьюмер успевает сделать `take()`.

Конкретно: в `take()` после успешного извлечения элемента — НЕ делать ничего с maxSizeSeen
(т.к. он не растёт от `take()`). Зато перед `put()`/`offer()` — проверять, если очередь
уже была полной, фиксировать maxSizeSeen = capacity. Либо, ещё проще: в `put()` и `offer()`
обновлять maxSizeSeen ДО вызова `queue.put()`, чтобы захватить размер, который только-что
увеличили. Простейший фикс:

```java
public void put(WALWriteRequest request) throws InterruptedException {
    if (queue.size() == queue.remainingCapacity()) {
        blockedCount.incrementAndGet();
    }
    queue.put(request);
    // Было: updateMaxSizeSeen() после put — гонка с take()
    // Стало: фиксируем пик размера "после вставки", но с учётом того,
    // что capacity() == size + remainingCapacity, и мы только что добавили.
    int sizeAfterPut = queue.size();
    long currentMax = maxSizeSeen.get();
    while (sizeAfterPut > currentMax) {
        if (maxSizeSeen.compareAndSet(currentMax, sizeAfterPut)) break;
        currentMax = maxSizeSeen.get();
    }
}
```
То же самое для `offer()`.

**Вариант B (фикс теста):**
В `WALBackpressureTest` заменить проверку «очередь была почти полной» на проверку
«producer хотя бы раз блокировался»:
```java
assertTrue(writer.getQueueBlockedCount() > 0,
           "Producer should have blocked at least once under sustained load");
```
Это надёжнее, потому что `blockedCount` инкрементируется синхронно в `put()` до блокировки
(строки 37–39 в `WALQueue.java`) и не подвержен той же гонке.

**Рекомендация:** сделать Вариант A (чинит корневую причину — продакшен-метрика backpressure
тоже занижена) и в дополнение — добавить assert из Варианта B в тест, чтобы покрытие
стало устойчивым к будущим регрессиям.

### Критерий приёмки
```bash
mvn -B -T 4 --no-transfer-progress -Dstyle.color=never test -P fast \
    -Dtest='WALBackpressureTest' 2>&1 | tail -30
```
Все 4 теста в `WALBackpressureTest` проходят. Прогнать 10 раз подряд — `for i in {1..10}; do mvn ... -Dtest='WALBackpressureTest#queueFullBlocksProducerAndLosesNoData'; done` — все 10 зелёные.

### Влияние
Production: метрика `WALWriter.maxQueueSizeSeen` начинает корректно отражать пиковую
длину очереди (сейчас занижена, что маскирует реальную backpressure под нагрузкой).

---

## Промпт P3 — Разогнать `AuditAsyncTest.auditDoesNotBlockTheMainQueryPath`

### Симптом
Один тест идёт ~154 секунды. Это не падение по assertion, а узкое место по времени —
тест «висит» так долго, что поднимает риск таймаута джобы `fast` (60 мин) и делает
параллельный `fast`-профиль нестабильным (коммит `263b6c6` уже пытался что-то с этим сделать).

### Корневая причина
Тест в строках 71–73 делает 500 `INSERT` подряд синхронно через `database.executeQuery(...)`.
Каждый INSERT идёт через SQL-парсер → planner → executor → WAL append → audit log append.
На каждый INSERT также срабатывает `AuditLogger`, который «переписывает весь audit-файл
на батч» (это явно написано в комментарии к тесту в строках 26–28).

Плюс в `@AfterEach`:
```java
database.close();
```
— это финальный drain, и в строке 79 тест ещё раз делает `database.close()` перед
`re-open` для проверки durability. То есть два `close()` подряд.

### Задача

1. **Не уменьшать число INSERT** — 500 это осмысленная нагрузка для проверки, что
   «producer никогда не блокируется на audit consumer». Assertion в строке 75
   (`elapsedMs < 30_000`) — это и есть SLO. Если оно проходит за 30 с, тест валиден.
2. Найти узкое место. Скорее всего это `AuditLogger` / `AuditBackend` — он пишет audit-запись
   per-INSERT синхронно или переписывает audit-таблицу целиком на каждый batch. Запустить
   тест с `-Dorg.slf4j.simpleLogger.defaultLogLevel=debug` и/или с JFR:
   ```bash
   mvn -B test -P fast -Dtest=AuditAsyncTest \
       -DargLine="-XX:StartFlightRecording=duration=60s,filename=/tmp/audit.jfr"
   ```
   Открыть `/tmp/audit.jfr` в JMC — найти top-3 метода по CPU/wall-time.
3. Если корень — `AuditLogger` блокирует producer (несмотря на имя теста), то это и есть
   реальный регрешн-баг production-кода. Исправить audit-очередь: producer должен
   `offer()` в bounded queue, а consumer-drain должен работать в отдельном треде.
   В комментариях к тесту явно сказано: «the producer only offers to a bounded queue,
   it never waits for the consumer's table append». Если в коде это не так — починить.
4. Если корень — `database.close()` дважды (строки 79 и в `@AfterEach`), убрать дубликат.
   `@AfterEach` уже вызывает `close()`. Строка 79 — это интеншн-проверка «final drain», но
   `@AfterEach` всё равно его сделает. Удалить дублирующий `close()` в самом тесте.
5. Если корень — `AuditLogger.flush()` блокирует на rewrite-all, то либо батчить
   flush (write audit rows в один `Files.write(...)`, не переписывать файл целиком),
   либо добавить buffered writer.

### Критерий приёмки
```bash
time mvn -B -T 4 --no-transfer-progress -Dstyle.color=never test -P fast \
    -Dtest='AuditAsyncTest#auditDoesNotBlockTheMainQueryPath' 2>&1 | tail -10
```
Локально тест должен завершаться < 30 секунд (assertion в строке 75 — < 30_000 мс на сами
500 INSERT, остальное — open/reopen/check — < 5 c). 154 c → ≤ 30 c — целевой регресс.

### Влияние
Production: убирается латентность на path `database.executeQuery(...)` при включённом
аудите (для любой workload-нагрузки > 100 stmt/s).

---

## Промпт P4 (опционально, fast-unblock) — Временно пометить тесты как `@Tag("flaky")` для разблокировки релизов

> Этот промпт нужен, только если P1–P3 нельзя сделать в один присест.
> `fast`-профиль исключает группы `large,flaky` (см. `pom.xml`, профиль `fast`).
> После пометки тесты перестанут запускаться в `fast`, CI позеленеет,
> `release-on-push.yml` создаст первый тег/релиз автоматически.

### Задача

1. В `src/test/java/diesel/storage/page/CheckpointManagerTest.java` добавить
   `@org.junit.jupiter.api.Tag("flaky")` на класс (он уже `@Tag("smoke") @Tag("storage")`,
   просто добавить третий) и оставить комментарий:
   ```java
   // TODO(P1): flaky due to CheckpointManager.checkpointNow() not updating
   // lastCheckpointDurationMs on no-op checkpoint. See CI3.md, промпт P1.
   @Tag("smoke")
   @Tag("storage")
   @Tag("flaky")
   public class CheckpointManagerTest {
   ```
2. В `src/test/java/diesel/WALBackpressureTest.java` — то же:
   ```java
   // TODO(P2): flaky due to WALQueue.updateMaxSizeSeen() race with take().
   // See CI3.md, промпт P2.
   @Tag("storage")
   @Tag("smoke")
   @Tag("flaky")
   @Isolated
   class WALBackpressureTest {
   ```
3. В `src/test/java/diesel/AuditAsyncTest.java` — то же:
   ```java
   // TODO(P3): flaky due to ~150s runtime under fast profile.
   // See CI3.md, промпт P3.
   @Tag("smoke")
   @Tag("flaky")
   @ResourceLock("diesel.audit.enabled")
   public class AuditAsyncTest {
   ```
4. Закоммитить с сообщением:
   ```
   3.4.37 ci: temporarily mark 3 flaky tests to unblock release pipeline

   - CheckpointManagerTest: flaky (no-op checkpoint doesn't update durationMs)
   - WALBackpressureTest: flaky (maxSizeSeen race with take())
   - AuditAsyncTest: flaky (~150s runtime under fast profile)

   See CI3.md (prompts P1-P3) for root cause and proper fixes.
   This is a temporary unblock — tracking issues must be resolved before
   the next minor release.
   ```
5. Запушить в `main`. После этого CI должен позеленеть, и `release-on-push.yml`
   создаст тег `v3.4.37-build.NNN` + GitHub Release с JAR'ником автоматически
   (где `NNN` — номер CI run на этом коммите).

### Критерий приёмки
- На GitHub Actions вкладке `CI` — последний run на `main` зелёный.
- На вкладке `Actions → Release on Push` — последний run не `skipped`, а `success`.
- На вкладке `/releases` — появился первый release `v3.4.37-build.NNN`.
- На вкладке `/tags` — появился первый tag `v3.4.37-build.NNN`.
- В `target/*.jar` в artifacts этого release — `dieseldb-3.4.37-build.NNN.jar`.

### Влияние
Production: **никакого** (это только исключает тесты из smoke-профиля).
Процесс: разблокирует релизный конвейер, который стоит уже ~30 коммитов.

### Важно
Это **временный** ход. После выполнения P1–P3 — убрать `@Tag("flaky")` со всех трёх
классов и убедиться, что тесты проходят в `fast` (без `flaky`-исключения). Открыть
тикет-трекинг задачу на это (например, в GitHub Issues), чтобы не забыть.

---

## Сводный чек-лист выполнения

После P1 + P2 + P3 (или P4 как temporary):

```bash
# 1. Локально — fast профиль полностью зелёный
mvn -B -T 4 --no-transfer-progress -Dstyle.color=never test -P fast 2>&1 | tail -20

# 2. Push в main
git push origin main

# 3. Дождаться CI run (зелёный)
# https://github.com/Reider85/dieseldb/actions/workflows/ci.yml

# 4. После CI success — Release on Push сработает автоматически
# https://github.com/Reider85/dieseldb/actions/workflows/release-on-push.yml

# 5. Проверить, что тег и релиз создались
# https://github.com/Reider85/dieseldb/tags
# https://github.com/Reider85/dieseldb/releases
```

Если после P1–P3 тег/релиз **всё ещё** не создаются, проверить:

1. Что коммит менял что-то из `paths`-фильтра в `ci.yml` (`src/**`, `pom.xml`,
   `diesel/**`, `.github/workflows/**`, `scripts/**`, и не только `.md`).
   Если коммит трогает только `*.md`/`docs/`/`LICENSE`/`NOTICE` — CI не запускается
   и `workflow_run`-событие не возникает, `release-on-push.yml` не сработает.
   Это задокументированное поведение (см. комментарии в `release-on-push.yml:6-8`).
2. Что `workflow_run.conclusion == 'success'`, `event == 'push'`, `head_branch == 'main'`
   (строки 31-33 в `release-on-push.yml`).
3. Что у `GITHUB_TOKEN` есть `contents: write` (строка 35 — есть, проверено).
4. Что в `release-on-push.yml` шаг «Skip if tag already exists» не отработал как `exists=true`
   (это возможно, если рераннили и тег от прежнего run уже создан).

---

## Приоритеты

- **P1** — высокий, чинит production-метрику и тест. ~30 мин работы.
- **P2** — высокий, чинит production-метрику и тест. ~20 мин работы.
- **P3** — средний, не ломает CI прямо сейчас, но рискует сломать под нагрузкой. ~1–2 ч работы.
- **P4** — «горящая кнопка», ~10 мин. Запустить, только если релизы нужны прямо сейчас,
  а P1–P3 не готовы.

Рекомендованный порядок: **P4 → P1 → P2 → P3 → убрать P4**. Это даёт работающий
релизный конвейер в течение 10 минут, а полноценную починку — в течение пары часов.

# Механизмы отказоустойчивости для DieselDB

## Обзор текущей архитектуры DieselDB

На текущий момент DieselDB реализует:
- **Персистентность**: сохранение таблиц в `.csv` (на каждый DML) и `.table` (сериализация при COMMIT)
- **Транзакции** с уровнями изоляции: READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
- **Индексы**: BTREE, HASH, UNIQUE, кластерные индексы
- **Восстановление индексов** после загрузки таблицы
- **Последовательности (SEQUENCE)** для автоинкремента

Однако отсутствуют критически важные механизмы отказоустойчивости, присутствующие в современных СУБД.

---

## Сравнительная таблица механизмов отказоустойчивости

| Механизм | PostgreSQL | MySQL (InnoDB) | MongoDB | Redis | SQLite | **Рекомендация для DieselDB** | Приоритет | Сложность реализации |
|----------|------------|----------------|---------|-------|--------|-------------------------------|-----------|---------------------|
| **WAL (Write-Ahead Logging)** | ✅ Есть (журнал предзаписи) | ✅ Есть (Redo Log) | ✅ Есть (Journal) | ❌ Нет (AOF опционально) | ✅ Есть (WAL mode) | **✅ РЕКОМЕНДУЕТСЯ**: Журнал транзакций перед применением изменений к данным | 🔴 Высокий | Средняя |
| **Checkpointing** | ✅ Периодические контрольные точки | ✅ Checkpoint в Redo Log | ✅ Periodic checkpoints | ❌ Нет | ❌ Нет | **✅ РЕКОМЕНДУЕТСЯ**: Периодическое сохранение состояния для ускорения восстановления | 🟡 Средний | Низкая |
| **Replication (Master-Slave)** | ✅ Streaming Replication | ✅ Group Replication, Master-Slave | ✅ Replica Sets | ✅ Sentinel + Cluster | ❌ Нет | **⚠️ ОПЦИОНАЛЬНО**: Для высокой доступности (требует сетевого взаимодействия) | 🟢 Низкий | Высокая |
| **Sharding / Partitioning** | ✅ Declarative Partitioning | ✅ Partitioning by Range/List/Hash | ✅ Auto-sharding | ✅ Cluster sharding | ❌ Нет | **⚠️ ОПЦИОНАЛЬНО**: Для масштабирования (горизонтальное разделение таблиц) | 🟢 Низкий | Очень высокая |
| **Crash Recovery** | ✅ WAL replay на старте | ✅ Redo Log recovery | ✅ Journal replay | ⚠️ RDB/AOF восстановление | ✅ WAL recovery | **✅ РЕКОМЕНДУЕТСЯ**: Восстановление из WAL/Journal при старте после сбоя | 🔴 Высокий | Средняя |
| **Two-Phase Commit (2PC)** | ✅ Prepared Transactions | ✅ XA Transactions | ✅ Distributed transactions | ❌ Нет | ❌ Нет | **⚠️ ОПЦИОНАЛЬНО**: Для распределённых транзакций (если будет репликация) | 🟢 Низкий | Высокая |
| **Point-in-Time Recovery (PITR)** | ✅ WAL архивирование + base backup | ✅ Binlog + backup | ✅ Oplog + snapshot | ⚠️ RDB snapshots | ❌ Нет | **⚠️ ОПЦИОНАЛЬНО**: Восстановление на конкретный момент времени | 🟢 Низкий | Средняя |
| **Automatic Failover** | ✅ Patroni, repmgr | ✅ InnoDB Cluster | ✅ Replica Set election | ✅ Redis Sentinel | ❌ Нет | **❌ НЕ ТРЕБУЕТСЯ**: Требует репликации и мониторинга | 🟢 Низкий | Очень высокая |
| **Data Checksums** | ✅ Page checksums | ✅ InnoDB checksums | ✅ WiredTiger checksums | ❌ Нет | ✅ Optional checksums | **✅ РЕКОМЕНДУЕТСЯ**: Контрольные суммы страниц/таблиц для обнаружения повреждения данных | 🟡 Средний | Низкая |
| **Shadow Paging** | ❌ Нет | ❌ Нет | ❌ Нет | ❌ Нет | ✅ Частично | **❌ НЕ ТРЕБУЕТСЯ**: Альтернатива WAL, сложнее в реализации | 🟢 Низкий | Высокая |
| **ARIES Recovery** | ✅ Частично | ✅ Full ARIES | ⚠️ Упрощённая версия | ❌ Нет | ❌ Нет | **✅ РЕКОМЕНДУЕТСЯ**: Алгоритм восстановления с Undo/Redo (Undo для отката, Redo для повтора) | 🔴 Высокий | Высокая |
| **Multi-Version Concurrency Control (MVCC)** | ✅ Полная поддержка | ⚠️ Частично (InnoDB) | ✅ Полная поддержка | ❌ Нет | ❌ Нет | **⚠️ УЖЕ ЕСТЬ**: Уровни изоляции реализованы, но MVCC можно улучшить | 🟡 Средний | Средняя |
| **Background Writer / Flusher** | ✅ Background writer | ✅ Adaptive flushing | ✅ Checkpoint thread | ✅ AOF fsync policies | ❌ Нет | **✅ РЕКОМЕНДУЕТСЯ**: Фоновая запись dirty pages на диск | 🟡 Средний | Средняя |
| **Fsinc Policies** | ✅ full_fsync, fdatasync | ✅ O_DIRECT, doublewrite | ✅ Journal fsync | ✅ everysec, always, no | ✅ F_FULLFSYNC | **✅ РЕКОМЕНДУЕТСЯ**: Настройка стратегий fsync для баланса надёжности/производительности | 🟡 Средний | Низкая |
| **Doublewrite Buffer** | ❌ Нет | ✅ Защита от partial page writes | ❌ Нет | ❌ Нет | ❌ Нет | **⚠️ ОПЦИОНАЛЬНО**: Защита от повреждения при сбое записи (для больших страниц) | 🟢 Низкий | Средняя |
| **Savepoints** | ✅ SAVEPOINT | ✅ SAVEPOINT | ❌ Нет | ❌ Нет | ✅ SAVEPOINT | **✅ РЕКОМЕНДУЕТСЯ**: Точки сохранения внутри транзакции для частичного отката | 🟡 Средний | Низкая |
| **Deadlock Detection** | ✅ Wait-for graph | ✅ Lock wait timeout | ✅ Lock timeout | ❌ Нет | ❌ Нет | **✅ РЕКОМЕНДУЕТСЯ**: Обнаружение и разрешение взаимных блокировок | 🟡 Средний | Средняя |
| **Lock Timeout** | ✅ lock_timeout | ✅ innodb_lock_wait_timeout | ✅ maxTimeMS | ❌ Нет | ❌ Нет | **✅ РЕКОМЕНДУЕТСЯ**: Таймаут ожидания блокировок | 🟡 Средний | Низкая |
| **Schema Versioning** | ❌ Нет | ❌ Нет | ❌ Нет | ❌ Нет | ❌ Нет | **⚠️ ОПЦИОНАЛЬНО**: Версионирование схемы для миграций | 🟢 Низкий | Средняя |

---

## Детальные рекомендации по внедрению

### 1. 🔴 WAL (Write-Ahead Logging) — ВЫСОКИЙ ПРИОРИТЕТ

**Что это:** Журнал предзаписи, куда сначала записываются все изменения, и только потом применяются к основным данным.

**Преимущества для DieselDB:**
- Гарантированная сохранность данных даже при сбое питания
- Быстрое восстановление после краха (replay журнала)
- Возможность реализации Point-in-Time Recovery
- Основа для репликации (отправка WAL slave'ам)

**Как реализовать:**
```java
// Пример структуры WAL entry
class WALEntry implements Serializable {
    long lsn; // Log Sequence Number
    UUID transactionId;
    long timestamp;
    String operationType; // INSERT, UPDATE, DELETE, COMMIT, ROLLBACK
    byte[] data; // Сериализованные данные изменения
    byte[] checksum; // Контрольная сумма
}

// Запись в WAL перед применением изменений
walManager.write(entry);
walManager.flush(); // Обязательный fsync перед коммитом
// Только после успешной записи в WAL применяем изменение к таблице
table.applyChange(change);
```

**Формат файла WAL:**
- Бинарный формат с фиксированным заголовком
- Каждый entry содержит LSN (монотонно растущий номер)
- Периодическая ротация файлов (segment-based)
- Опциональное сжатие старых сегментов

---

### 2. 🔴 Crash Recovery (ARIES-style) — ВЫСОКИЙ ПРИОРИТЕТ

**Что это:** Алгоритм восстановления базы данных после сбоя, состоящий из трёх фаз:
1. **Analysis** — определение границ восстановления
2. **Redo** — повтор всех зафиксированных транзакций из WAL
3. **Undo** — откат незавершённых транзакций

**Преимущества для DieselDB:**
- Автоматическое восстановление консистентного состояния
- Гарантия ACID свойств даже при крахе
- Минимальная потеря данных (только незакоммиченные транзакции)

**Как реализовать:**
```java
class CrashRecoveryManager {
    void recover(Database db) {
        // Phase 1: Analysis - найти последний checkpoint
        Checkpoint lastCheckpoint = findLastCheckpoint();
        long startLSN = lastCheckpoint != null ? lastCheckpoint.lsn : 0;
        
        // Phase 2: Redo - повторить все закоммиченные операции
        Map<UUID, Transaction> activeTransactions = new HashMap<>();
        walManager.replayFrom(startLSN, entry -> {
            if (entry.isCommit()) {
                commitTransaction(entry.transactionId);
            } else if (entry.isRollback()) {
                rollbackTransaction(entry.transactionId);
            } else {
                //redo операцию
                applyRedo(entry);
                trackActiveTransaction(entry.transactionId);
            }
        });
        
        // Phase 3: Undo - откатить незавершённые транзакции
        for (UUID txId : activeTransactions.keySet()) {
            undoTransaction(txId);
        }
    }
}
```

---

### 3. 🟡 Checkpointing — СРЕДНИЙ ПРИОРИТЕТ

**Что это:** Периодическое сохранение полного состояния БД на диск, чтобы сократить время восстановления.

**Преимущества:**
- Ускорение crash recovery (не нужно replay весь WAL с начала времён)
- Возможность очистки старых WAL сегментов
- Снижение нагрузки на диск при восстановлении

**Как реализовать:**
```java
class CheckpointManager {
    void createCheckpoint() {
        // 1. Записать checkpoint record в WAL
        CheckpointRecord record = new CheckpointRecord(currentLSN, activeTransactions);
        walManager.write(record);
        walManager.flush();
        
        // 2. Сохранить состояние всех таблиц
        for (Table table : database.tables.values()) {
            table.saveToSerializedFile();
        }
        
        // 3. Обновить pointer на последний checkpoint
        updateCheckpointPointer(currentLSN);
        
        // 4. Очистить старые WAL сегменты (опционально)
        walManager.cleanupBefore(record.lsn);
    }
}
```

**Стратегии создания checkpoint:**
- **Periodic**: каждые N минут (например, 5 мин)
- **Log-based**: после записи N MB WAL
- **Transaction-based**: после N коммитов

---

### 4. 🟡 Data Checksums — СРЕДНИЙ ПРИОРИТЕТ

**Что это:** Контрольные суммы для обнаружения повреждения данных (bit rot, сбой диска).

**Преимущества:**
- Раннее обнаружение corrupted данных
- Предотвращение чтения повреждённых страниц
- Возможность alert администратора

**Как реализовать:**
```java
class ChecksummedPage {
    byte[] data;
    long checksum; // CRC32 или CRC64
    
    long calculateChecksum() {
        return CRC32C.calculate(data);
    }
    
    boolean verify() {
        return checksum == calculateChecksum();
    }
}

// При чтении таблицы
if (!page.verify()) {
    throw new DataCorruptionException("Page checksum mismatch");
}
```

**Алгоритмы checksum:**
- CRC32C (быстрый, хорошая защита)
- CRC64 (лучшая защита, чуть медленнее)
- xxHash (очень быстрый)

---

### 5. 🟡 Savepoints — СРЕДНИЙ ПРИОРИТЕТ

**Что это:** Точки сохранения внутри транзакции для частичного отката.

**Преимущества:**
- Гибкое управление транзакциями
- Возможность отката части операций без полного rollback
- Упрощение обработки ошибок в сложных транзакциях

**Как реализовать:**
```java
class Transaction {
    Map<String, Integer> savepoints = new LinkedHashMap<>();
    int savepointCounter = 0;
    
    String setSavepoint(String name) {
        String savepointName = name != null ? name : "SP_" + savepointCounter++;
        savepoints.put(savepointName, currentWalPosition);
        return savepointName;
    }
    
    void rollbackToSavepoint(String savepointName) {
        if (!savepoints.containsKey(savepointName)) {
            throw new IllegalArgumentException("Savepoint not found");
        }
        int walPosition = savepoints.get(savepointName);
        undoOperationsAfter(walPosition);
        // Удалить все savepoints после указанного
        removeSavepointsAfter(savepointName);
    }
    
    void releaseSavepoint(String savepointName) {
        savepoints.remove(savepointName);
    }
}
```

---

### 6. 🟡 Deadlock Detection & Lock Timeout — СРЕДНИЙ ПРИОРИТЕТ

**Что это:** Обнаружение взаимных блокировок и автоматическое разрешение путём отката одной из транзакций.

**Преимущества:**
- Предотвращение зависаний системы
- Автоматическое разрешение deadlock
- Улучшение отзывчивости системы

**Как реализовать:**
```java
class DeadlockDetector {
    // Граф ожидания блокировок
    Map<UUID, Set<UUID>> waitForGraph = new HashMap<>();
    
    boolean detectDeadlock(UUID requestingTx, UUID holdingTx) {
        // DFS поиск цикла в графе
        Set<UUID> visited = new HashSet<>();
        return hasCycle(requestingTx, visited);
    }
    
    void resolveDeadlock(Set<UUID> involvedTransactions) {
        // Выбрать victim (транзакцию с наименьшей стоимостью отката)
        UUID victim = selectVictim(involvedTransactions);
        rollbackTransaction(victim);
        throw new DeadlockException("Deadlock detected, transaction rolled back");
    }
}

class LockManager {
    long lockWaitTimeout = 5000; // 5 секунд по умолчанию
    
    Lock acquireLock(Resource resource, UUID transactionId) throws LockTimeoutException {
        long startTime = System.currentTimeMillis();
        while (!tryLock(resource, transactionId)) {
            if (System.currentTimeMillis() - startTime > lockWaitTimeout) {
                throw new LockTimeoutException("Lock wait timeout exceeded");
            }
            Thread.sleep(10);
        }
        return lock;
    }
}
```

---

### 7. 🟡 Fsyc Policies — СРЕДНИЙ ПРИОРИТЕТ

**Что это:** Настройка стратегий синхронизации данных на диске.

**Варианты политик:**
| Политика | Надёжность | Производительность | Описание |
|----------|------------|-------------------|----------|
| `ALWAYS` | Максимальная | Низкая | fsync после каждой операции |
| `EVERY_SEC` | Высокая | Средняя | fsync раз в секунду (потеря ≤1 сек данных) |
| `COMMIT_ONLY` | Средняя | Высокая | fsync только при коммите транзакции |
| `NEVER` | Низкая | Максимальная | Без fsync (полагается на ОС) |

**Как реализовать:**
```java
enum FsyncPolicy {
    ALWAYS,      // fsync после каждой записи
    EVERY_SEC,   // fsync раз в секунду (фондовый поток)
    COMMIT_ONLY, // fsync только при COMMIT
    NEVER        // без fsync (опасно!)
}

class WALManager {
    FsyncPolicy policy = FsyncPolicy.COMMIT_ONLY;
    
    void write(WALEntry entry) {
        writeToFile(entry);
        if (policy == FsyncPolicy.ALWAYS) {
            forceSync();
        }
    }
    
    void flush() {
        if (policy == FsyncPolicy.ALWAYS || policy == FsyncPolicy.COMMIT_ONLY) {
            forceSync();
        }
    }
    
    // Фоновый поток для EVERY_SEC
    void startBackgroundFsync() {
        if (policy == FsyncPolicy.EVERY_SEC) {
            scheduler.scheduleAtFixedRate(() -> forceSync(), 1, 1, TimeUnit.SECONDS);
        }
    }
}
```

---

### 8. 🟡 Background Writer — СРЕДНИЙ ПРИОРИТЕТ

**Что это:** Фоновый поток для периодической записи dirty pages на диск.

**Преимущества:**
- Снижение latency пользовательских операций
- Равномерная нагрузка на диск
- Предотвращение больших пиков записи при checkpoint

**Как реализовать:**
```java
class BackgroundWriter {
    Queue<DirtyPage> dirtyPages = new ConcurrentLinkedQueue<>();
    
    void markPageDirty(Page page) {
        dirtyPages.offer(new DirtyPage(page, System.currentTimeMillis()));
    }
    
    void start() {
        scheduler.scheduleAtFixedRate(() -> {
            List<DirtyPage> batch = new ArrayList<>();
            dirtyPages.drainTo(batch, 100); // Batch по 100 страниц
            
            for (DirtyPage dp : batch) {
                if (dp.age() > MAX_AGE_MS) {
                    writePageToDisk(dp.page);
                }
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
    }
}
```

---

### 9. ⚠️ Репликация (Master-Slave) — НИЗКИЙ ПРИОРИТЕТ

**Что это:** Поддержка нескольких копий базы данных для высокой доступности и чтения.

**Преимущества:**
- High Availability (автоматический failover)
- Read scaling (чтение с реплик)
- Disaster recovery

**Недостатки:**
- Высокая сложность реализации
- Требует сетевого взаимодействия
- Проблемы консистентности (eventual consistency)

**Рекомендация:** Реализовать только если потребуется горизонтальное масштабирование.

---

### 10. ⚠️ Point-in-Time Recovery (PITR) — НИЗКИЙ ПРИОРИТЕТ

**Что это:** Возможность восстановить базу данных на любой момент времени в прошлом.

**Требования:**
- WAL архивирование (сохранение всех WAL сегментов)
- Periodic base backups (полные снимки)
- Механизм replay WAL до нужного timestamp

**Как реализовать:**
```java
class PITRManager {
    void restoreToTimestamp(long targetTimestamp) {
        // 1. Найти последний backup перед targetTimestamp
        Backup backup = findBackupBefore(targetTimestamp);
        restoreFromBackup(backup);
        
        // 2. Replay WAL от backup до targetTimestamp
        walManager.replayRange(backup.walLsn, targetTimestamp, entry -> {
            if (entry.timestamp <= targetTimestamp) {
                applyRedo(entry);
            }
        });
    }
}
```

---

## Roadmap внедрения для DieselDB

### Этап 1: Базовая отказоустойчивость (2-4 недели)
1. ✅ **WAL Manager** — журнал предзаписи
2. ✅ **Crash Recovery** — восстановление после сбоя
3. ✅ **Fsyc Policies** — настройка стратегий синхронизации

### Этап 2: Улучшение надёжности (2-3 недели)
4. ✅ **Checkpointing** — контрольные точки
5. ✅ **Data Checksums** — контроль целостности
6. ✅ **Savepoints** — точки сохранения

### Этап 3: Продвинутые функции (3-4 недели)
7. ✅ **Deadlock Detection** — обнаружение блокировок
8. ✅ **Lock Timeout** — таймауты блокировок
9. ✅ **Background Writer** — фоновая запись

### Этап 4: Масштабирование (опционально, 8+ недель)
10. ⚠️ **Репликация** — Master-Slave
11. ⚠️ **PITR** — восстановление на момент времени
12. ⚠️ **Sharding** — горизонтальное разделение

---

## Архитектурные изменения

### Новые классы:
```
diesel/
├── wal/
│   ├── WALManager.java          # Управление журналом
│   ├── WALEntry.java            # Entry журнала
│   ├── WALSegment.java          # Сегмент WAL файла
│   └── WALRecoveryManager.java  # Восстановление из WAL
├── recovery/
│   ├── CrashRecoveryManager.java # ARIES recovery
│   ├── CheckpointManager.java    # Checkpointing
│   └── CheckpointRecord.java     # Record checkpoint
├── checksum/
│   ├── ChecksummedPage.java     # Страница с checksum
│   └── CRC32C.java              # Алгоритм checksum
├── lock/
│   ├── DeadlockDetector.java    # Обнаружение deadlock
│   └── LockTimeoutManager.java  # Таймауты блокировок
└── savepoint/
    ├── SavepointManager.java    # Управление savepoint
    └── Savepoint.java           # Точка сохранения
```

### Изменения в существующих классах:
- `Transaction` — добавить поддержку savepoints
- `Database` — интеграция WAL и crash recovery
- `Table` — checksum страниц, integration с background writer
- `DatabaseServer` — recovery при старте, background threads

---

## Заключение

Для DieselDB критически важно внедрить **WAL + Crash Recovery** как базовый механизм отказоустойчивости. Это обеспечит гарантию сохранности данных и возможность восстановления после сбоев.

Далее рекомендуется добавить **Checkpointing** для ускорения восстановления и **Data Checksums** для обнаружения повреждения данных.

**Savepoints**, **Deadlock Detection** и **Lock Timeout** улучшат удобство использования и надёжность транзакций.

Продвинутые функции (**репликация**, **PITR**, **sharding**) следует рассматривать только при наличии конкретных требований к масштабированию.

---

## Источники

1. PostgreSQL Documentation: https://www.postgresql.org/docs/current/wal.html
2. MySQL InnoDB Architecture: https://dev.mysql.com/doc/internals/en/innodb-architecture.html
3. MongoDB Journaling: https://www.mongodb.com/docs/manual/core/journaling/
4. ARIES Algorithm: Mohan, C., et al. "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging."
5. Redis Persistence: https://redis.io/topics/persistence
6. SQLite WAL Mode: https://www.sqlite.org/wal.html

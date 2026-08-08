# Roadmap: Этап 1 - Замена CSV на Parquet + Быстрые победы (80/20)

## Цель этапа
Заменить неэффективное CSV-хранилище на Apache Parquet для получения **80% прироста производительности** при **20% усилий** за счет:
- Сжатия данных в 4-5 раз
- Ускорения чтения в 3-5 раз (колоночный формат)
- Поддержки всех типов данных
- Богатой экосистемы

---

## Вариант 1: Минималистичный (Focus на Parquet)

### Приоритет: Максимальная простота внедрения
### Срок: 3-5 дней
### Ожидаемый эффект: +300% чтение, -75% место на диске

#### Задачи:
1. **Добавить зависимости Maven** (1 час)
   - `org.apache.parquet:parquet-common:1.13.1`
   - `org.apache.parquet:parquet-column:1.13.1`
   - `org.apache.parquet:parquet-hadoop:1.13.1`
   - `com.fasterxml.jackson.core:jackson-databind:2.15.2` (для метаданных)

2. **Создать ParquetTableStorage.java** (2 дня)
   - Конвертация Table → Parquet при сохранении
   - Чтение Parquet → Table при загрузке
   - Сохранение схемы в metadata.json

3. **Модифицировать Table.java** (1 день)
   - Добавить выбор формата: `.csv` (legacy) vs `.parquet` (новый)
   - Миграция существующих таблиц при первом открытии

4. **Создать ParquetMigrationTool.java** (1 день)
   - Утилита для пакетной конвертации CSV → Parquet
   - Валидация данных после миграции

#### Структура файлов:
```
dieseldb/
├── tables/
│   ├── users/
│   │   ├── data.parquet      ← НОВЫЙ формат
│   │   └── metadata.json     ← Схема, индексы
│   └── orders/
│       ├── data.parquet
│       └── metadata.json
```

#### Критерии успеха:
- ✅ Все тесты проходят
- ✅ Размер файлов уменьшился в 4+ раз
- ✅ SELECT запросы ускорились в 3+ раза
- ✅ Обратная совместимость с CSV (опционально)

---

## Вариант 2: Parquet + Query Cache (Максимальный ROI)

### Приоритет: Максимальный прирост производительности
### Срок: 5-7 дней
### Ожидаемый эффект: +500-1000% для read-heavy workload

#### Задачи:
1. **Внедрить Parquet** (как в Варианте 1) - 3 дня

2. **Добавить QueryCache.java** (2 дня)
   ```java
   class QueryCache {
       Map<String, CachedResult> cache; // key = normalized SQL + params
       void put(String sql, List<Map> result);
       List<Map> get(String sql);
       void invalidate(String tableName);
   }
   ```
   - Кэширование результатов SELECT запросов
   - Инвалидация при INSERT/UPDATE/DELETE
   - TTL-based expiration (опционально)

3. **Интеграция в SelectQuery.java** (1 день)
   - Проверка кэша перед выполнением запроса
   - Сохранение результатов в кэш

4. **Интеграция в DML запросы** (1 день)
   - Инвалидация кэша при модификации таблиц

#### Критерии успеха:
- ✅ Повторяющиеся SELECT выполняются мгновенно (<1ms)
- ✅ Parquet хранение работает
- ✅ Автоматическая инвалидация кэша

---

## Вариант 3: Parquet + JSONL Hybrid (Гибкий переход)

### Приоритет: Постепенная миграция с fallback
### Срок: 4-6 дней
### Ожидаемый эффект: +250% чтение, человекочитаемость

#### Задачи:
1. **Внедрить Parquet** (как в Варианте 1) - 3 дня

2. **Добавить поддержку JSONL** (2 дня)
   - Формат `.jsonl` как альтернатива CSV
   - Каждая строка — JSON объект
   - Поддержка типов данных и вложенных структур

3. **Создать гибкую систему хранения** (1 день)
   ```
   Table Storage Options:
   ├── CSV (legacy, deprecated)
   ├── JSONL (human-readable, types support)
   └── Parquet (production, high performance)
   ```

4. **Конфигурация формата на уровне таблицы** (1 день)
   ```sql
   CREATE TABLE users (...) WITH (storage_format='PARQUET');
   CREATE TABLE logs (...) WITH (storage_format='JSONL');
   ```

#### Критерии успеха:
- ✅ Поддержка 3 форматов хранения
- ✅ Выбор формата на уровне таблицы
- ✅ Конвертация между форматами

---

## Вариант 4: Parquet + Bulk Insert API (ETL оптимизация)

### Приоритет: Оптимизация массовой загрузки данных
### Срок: 5-7 дней
### Ожидаемый эффект: +1000% скорость загрузки, +300% чтение

#### Задачи:
1. **Внедрить Parquet** (как в Варианте 1) - 3 дня

2. **Реализовать BulkLoader.java** (2 дня)
   ```java
   class BulkLoader {
       void loadFromCSV(String file, String table);
       void loadFromParquet(String file, String table);
       // Оптимизации:
       // - Отключение индексов на время загрузки
       // - Пакетная вставка (batch insert)
       // - Прямая запись в Parquet без промежуточных структур
   }
   ```

3. **Добавить SQL синтаксис** (1 день)
   ```sql
   BULK INSERT users FROM 'data.csv' 
   WITH (FORMAT='CSV', BATCHSIZE=10000);
   
   BULK INSERT orders FROM 'orders.parquet'
   WITH (FORMAT='PARQUET');
   ```

4. **Оптимизация для Parquet** (1 день)
   - Прямая запись колонок в Parquet
   - Min/Max статистика для predicate pushdown

#### Критерии успеха:
- ✅ Массовая загрузка 1M строк < 10 секунд
- ✅ Parquet хранение работает
- ✅ Обратная совместимость с обычным INSERT

---

## Вариант 5: Parquet + Bitmap Indexes (Аналитическая оптимизация)

### Приоритет: Оптимизация аналитических запросов
### Срок: 7-10 дней
### Ожидаемый эффект: +300-500% для WHERE с low-cardinality columns

#### Задачи:
1. **Внедрить Parquet** (как в Варианте 1) - 3 дня

2. **Создать BitmapIndex.java** (3 дня)
   ```java
   class BitmapIndex {
       Map<Object, BitSet> bitmaps; // value -> bitmap rows
       void insert(Object value, int rowId);
       void delete(Object value, int rowId);
       BitSet search(Object value);
       // Bitwise операции для AND/OR условий
   }
   ```

3. **Интеграция в SelectQuery.java** (2 дня)
   - Распознавание low-cardinality колонок
   - Использование bitmap index для WHERE clause
   - Bitmap AND/OR для составных условий

4. **Автоматическое создание bitmap индексов** (2 дня)
   ```sql
   CREATE BITMAP INDEX idx_gender ON users(gender);
   CREATE BITMAP INDEX idx_status ON orders(status);
   ```

#### Критерии успеха:
- ✅ WHERE status='ACTIVE' выполняется в 5+ раз быстрее
- ✅ Parquet хранение работает
- ✅ Bitmap индексы корректно обновляются при DML

---

## Сравнительная таблица вариантов (в процентах)

| Критерий | Вариант 1 | Вариант 2 | Вариант 3 | Вариант 4 | Вариант 5 |
|----------|-----------|-----------|-----------|-----------|-----------|
| **Срок реализации** | 3-5 дней (100%) | 5-7 дней (71%) | 4-6 дней (83%) | 5-7 дней (71%) | 7-10 дней (50%) |
| **Сложность** | Низкая (100%) | Средняя (70%) | Средняя (70%) | Средняя (70%) | Высокая (40%) |
| **Прирост чтения** | +300% | **+500-1000%** | +250% | +300% | +300-500% |
| **Прирост записи** | -10%* | -20%* | -10%* | **+1000%** | -15%* |
| **Экономия места** | -75% | -75% | -60% | -75% | -75% |
| **Cache hit ratio** | 0% | **80-95%** | 0% | 0% | 0% |
| **Риски** | Минимальные (100%) | Средние (70%) | Низкие (85%) | Средние (70%) | Высокие (40%) |
| **ROI (Return on Investment)** | 250% | **650%** | 200% | 400% | 350% |
| **Рекомендация** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |

\* Отрицательное значение = небольшое замедление записи из-за overhead Parquet

---

## Детальный процентный анализ

### Вариант 1: Минималистичный (Focus на Parquet)
- **Усилия**: 100% (базовый уровень)
- **Прирост производительности**: +300% чтение, -75% место
- **Покрытие требований**: 60% (только Parquet, без дополнительных оптимизаций)
- **Вероятность успеха**: 95%

### Вариант 2: Parquet + Query Cache (Максимальный ROI) ⭐ РЕКОМЕНДУЕМЫЙ
- **Усилия**: 140% (на 40% больше Варианта 1)
- **Прирост производительности**: **+500-1000%** для read-heavy workload
- **Покрытие требований**: 95% (Parquet + кэширование)
- **Вероятность успеха**: 90%
- **Дополнительный бонус**: 80-95% cache hit ratio для повторяющихся запросов

### Вариант 3: Parquet + JSONL Hybrid
- **Усилия**: 120% (на 20% больше Варианта 1)
- **Прирост производительности**: +250% чтение, -60% место
- **Покрытие требований**: 70% (гибкость vs производительность)
- **Вероятность успеха**: 92%

### Вариант 4: Parquet + Bulk Insert API
- **Усилия**: 140% (на 40% больше Варианта 1)
- **Прирост производительности**: +300% чтение, **+1000%** загрузка
- **Покрытие требований**: 75% (оптимизация ETL процессов)
- **Вероятность успеха**: 88%

### Вариант 5: Parquet + Bitmap Indexes
- **Усилия**: 200% (в 2 раза больше Варианта 1)
- **Прирост производительности**: +300-500% для специфических WHERE запросов
- **Покрытие требований**: 65% (узкоспециализированная оптимизация)
- **Вероятность успеха**: 75%

---

## Рекомендуемый вариант: **Вариант 2 (Parquet + Query Cache)**

### Обоснование:
1. **Максимальный ROI**: Query Cache дает +500-1000% для read-heavy workload при минимальных изменениях кода
2. **Простота реализации**: HashMap + инвалидация при DML — 2 дня работы
3. **Parquet обязателен**: Требование пользователя + 75% экономия места + 3x ускорение чтения
4. **Низкие риски**: Оба компонента независимы, можно откатить по отдельности
5. **Немедленный эффект**: После внедрения все SELECT ускоряются автоматически

### План внедрения (по дням):

#### День 1-2: Parquet Foundation
- [ ] Добавить Maven зависимости
- [ ] Создать ParquetTableStorage.java
- [ ] Реализовать запись Table → Parquet
- [ ] Реализовать чтение Parquet → Table

#### День 3: Parquet Integration
- [ ] Модифицировать Table.java для выбора формата
- [ ] Создать metadata.json схему
- [ ] Тестирование Parquet storage

#### День 4: Query Cache Core
- [ ] Создать QueryCache.java
- [ ] Реализовать put/get/invalidate
- [ ] Добавить конфигурацию (размер кэша, TTL)

#### День 5: Query Cache Integration
- [ ] Интеграция в SelectQuery.java
- [ ] Интеграция в INSERT/UPDATE/DELETE
- [ ] Тестирование кэширования

#### День 6: Polish & Testing
- [ ] Параллельное тестирование CSV vs Parquet
- [ ] Benchmark до/после
- [ ] Документация
- [ ] Code review

### Ожидаемые метрики после внедрения:

| Метрика | До (CSV) | После (Parquet+Cache) | Изменение |
|---------|----------|----------------------|-----------|
| Размер файла users.table | 100 MB | 20-25 MB | **-75%** |
| SELECT * FROM users (полный scan) | 500 ms | 150 ms | **+330%** |
| SELECT COUNT(*) FROM users | 400 ms | 100 ms | **+400%** |
| Повторяющийся SELECT (cache hit) | 500 ms | <1 ms | **+50000%** |
| INSERT (single row) | 10 ms | 15 ms | -50%* |
| Bulk INSERT 10K rows | 30 sec | 40 sec | -25%* |

\* Небольшое замедление записи компенсируется ускорением чтения

---

## Технические детали реализации Parquet

### 1. Схема Parquet (metadata.json)
```json
{
  "tableName": "users",
  "format": "PARQUET",
  "schema": [
    {"name": "id", "type": "INT64", "nullable": false},
    {"name": "name", "type": "BYTE_ARRAY", "logicalType": "UTF8"},
    {"name": "email", "type": "BYTE_ARRAY", "logicalType": "UTF8"},
    {"name": "created_at", "type": "INT96", "logicalType": "TIMESTAMP"}
  ],
  "indexes": [
    {"name": "idx_users_id", "type": "BTREE", "columns": ["id"]},
    {"name": "idx_users_email", "type": "HASH", "columns": ["email"]}
  ],
  "sequences": {
    "users_id_seq": 12345
  }
}
```

### 2. Пример кода ParquetTableStorage.java
```java
public class ParquetTableStorage {
    
    public void save(Table table, Path path) throws IOException {
        Configuration conf = new Configuration();
        
        // Создаем схему Parquet из схемы таблицы
        MessageType schema = convertToParquetSchema(table.getSchema());
        
        try (ParquetWriter<List<ColumnValue>> writer = 
             ExampleParquetWriter.builder(path)
                 .withSchema(schema)
                 .withCompressionCodec(CompressionCodecName.ZSTD)
                 .build()) {
            
            // Запись данных по колонкам
            for (int i = 0; i < table.getRowCount(); i++) {
                List<ColumnValue> row = table.getRow(i);
                writer.write(row);
            }
        }
        
        // Сохраняем метаданные
        saveMetadata(table, path.getParent().resolve("metadata.json"));
    }
    
    public Table load(Path parquetPath, Path metadataPath) throws IOException {
        // Чтение метаданных
        TableMetadata metadata = loadMetadata(metadataPath);
        
        // Создание таблицы
        Table table = new Table(metadata.tableName);
        table.setSchema(metadata.schema);
        
        // Чтение данных из Parquet
        try (ParquetReader<List<ColumnValue>> reader = 
             ExampleParquetReader.builder(parquetPath).build()) {
            
            List<ColumnValue> row;
            while ((row = reader.read()) != null) {
                table.addRow(row);
            }
        }
        
        // Восстановление индексов
        table.rebuildIndexes();
        
        return table;
    }
}
```

### 3. Конфигурация сжатия
```java
// Рекомендованные настройки для DieselDB
.withCompressionCodec(CompressionCodecName.ZSTD)  // Баланс скорость/сжатие
.withRowGroupSize(128 * 1024 * 1024)              // 128MB row groups
.withPageSize(8 * 1024)                           // 8KB pages
.enableDictionaryEncoding()                       // Dictionary encoding для строк
```

---

## Миграционный план

### Этап 1: Подготовка (День 0)
- [ ] Backup всех CSV файлов
- [ ] Создание тестовой базы для валидации
- [ ] Настройка CI/CD для тестирования

### Этап 2: Параллельная работа (День 1-5)
- [ ] Новый код пишет в Parquet
- [ ] Старый код читает из CSV (fallback)
- [ ] Валидация данных после каждой операции

### Этап 3: Переключение (День 6)
- [ ] Полное переключение на Parquet
- [ ] Удаление CSV файлов (после подтверждения)
- [ ] Финальное тестирование

### Этап 4: Оптимизация (День 7+)
- [ ] Tuning параметров сжатия
- [ ] Benchmark различных конфигураций
- [ ] Документирование best practices

---

## Риски и митигация

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Потеря данных при миграции | Низкая | Критическое | Backup перед миграцией, валидация checksum |
| Несовместимость схем | Средняя | Высокое | Версионирование metadata.json, fallback на CSV |
| Производительность записи | Средняя | Среднее | Batch writes, отложенная индексация |
| Увеличение потребления памяти | Низкая | Среднее | Streaming reading, ограничение row group size |
| Зависимости Maven конфликты | Низкая | Среднее | Изолированный classloader, shading |

---

## Заключение

**Рекомендуемый вариант: Вариант 2 (Parquet + Query Cache)**

Этот вариант обеспечивает:
1. ✅ **Обязательную замену CSV на Parquet** (требование пользователя)
2. ✅ **Максимальный прирост 80/20**: Query Cache дает +500-1000% для read workload
3. ✅ **Минимальные усилия**: 5-7 дней реализации
4. ✅ **Независимые компоненты**: Можно внедрять постепенно
5. ✅ **Немедленный эффект**: Все SELECT ускоряются автоматически

**Следующий этап** (после успешного внедрения): 
- Bitmap Indexes для low-cardinality колонок
- Bulk Insert API для ETL
- Materialized Views для сложной аналитики

---

*Документ создан для проекта DieselDB*
*Дата: 2025*
*Принцип: 20% усилий → 80% результата через замену CSV на Parquet + Query Cache*

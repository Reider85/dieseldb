# Функции мониторинга для DieselDB

Этот документ описывает рекомендуемые функции мониторинга для DieselDB, расставленные по приоритетам от критических до перспективных.

---

## Приоритет 1: Критический (Must Have)

Эти функции необходимы для базовой работоспособности и диагностики проблем в production.

### 1.1 Метрики производительности запросов
**Описание:** Сбор статистики по времени выполнения запросов
- Среднее время выполнения запроса
- Минимальное/максимальное время выполнения
- 95-й и 99-й перцентили времени отклика
- Количество запросов в секунду (QPS)
- Гистограмма распределения времени выполнения

**Зачем:** Позволяет выявлять медленные запросы, деградацию производительности, узкие места.

**Реализация:**
```java
// В Database.executeQuery()
private final AtomicLong queryCount = new AtomicLong();
private final AtomicLong totalQueryTime = new AtomicLong();
private final Histogram queryTimeHistogram; // или аналогичная структура
```

### 1.2 Метрики подключений
**Описание:** Мониторинг пула соединений и активных клиентов
- Количество активных подключений
- Количество ожидающих подключений (в очереди)
- Максимальное количество подключений (pool size)
- Количество отклонённых подключений (RejectedExecutionException)
- Время жизни подключения

**Зачем:** Предотвращение исчерпания пула соединений, выявление утечек подключений.

**Реализация:** Использовать `ThreadPoolExecutor` метрики:
```java
executor.getActiveCount();
executor.getPoolSize();
executor.getQueue().size();
((ArrayBlockingQueue) executor.getQueue()).remainingCapacity();
```

### 1.3 Метрики транзакций
**Описание:** Статистика по активным и завершённым транзакциям
- Количество активных транзакций
- Длительность активной транзакции (max, avg)
- Количество коммитов/роллбеков
- Транзакции, длящиеся дольше порога (long-running transactions)
- Распределение по уровням изоляции

**Зачем:** Выявление длительных транзакций, блокировок, проблем с ACID.

**Реализация:**
```java
// В классе Database
private final AtomicInteger activeTransactions = new AtomicInteger();
private final AtomicLong committedTransactions = new AtomicLong();
private final AtomicLong rolledBackTransactions = new AtomicLong();
// Map<UUID, Long> transactionStartTime для отслеживания длительности
```

### 1.4 Метрики памяти и ресурсов
**Описание:** Использование ресурсов JVM и системы
- Использование heap memory (used/max)
- Использование non-heap memory
- Количество потоков
- Загрузка CPU (process-level)
- GC статистика (частота, длительность пауз)

**Зачем:** Предотвращение OOM ошибок, оптимизация настроек JVM.

**Реализация:** JMX MBeans или Micrometer:
```java
MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
memoryBean.getHeapMemoryUsage();
memoryBean.getNonHeapMemoryUsage();
```

### 1.5 Логирование ошибок и исключений
**Описание:** Централизованный сбор и классификация ошибок
- Количество ошибок по типам (SQLException, IOException, etc.)
- Частота возникновения ошибок
- Контекст ошибки (запрос, таблица, транзакция)
- Stack trace для критических ошибок

**Зачем:** Быстрая диагностика сбоев, выявление паттернов ошибок.

**Реализация:** Расширить существующий `Logger`, добавить счётчики:
```java
private final ConcurrentMap<String, AtomicLong> errorCounts = new ConcurrentHashMap<>();
// В catch блоках: errorCounts.computeIfAbsent(errorType, k -> new AtomicLong()).incrementAndGet();
```

---

## Приоритет 2: Высокий (Should Have)

Эти функции важны для полноценного мониторинга production-системы.

### 2.1 Метрики таблиц и индексов
**Описание:** Статистика по объектам базы данных
- Количество строк в каждой таблице
- Размер таблицы на диске (.table файл)
- Количество индексов на таблицу
- Размер индексов
- Статистика использования индексов (hit rate)
- Fragmentation level для B-tree индексов

**Зачем:** Планирование ёмкости, оптимизация схемы БД, выявление неиспользуемых индексов.

**Реализация:**
```java
// В классе Table
public long getRowCount() { return rows.size(); }
public long getIndexCount() { return indexes.size(); }
public long getTableFileSize() { return new File(name + ".table").length(); }
```

### 2.2 Slow Query Log
**Описание:** Логирование запросов, превышающих порог времени
- Настраиваемый порог (например, >100ms)
- Полный текст запроса
- Время выполнения
- Контекст (транзакция, пользователь)

**Зачем:** Выявление проблемных запросов для оптимизации.

**Реализация:**
```java
private static final long SLOW_QUERY_THRESHOLD_MS = 100;
// После выполнения запроса:
long executionTime = System.currentTimeMillis() - startTime;
if (executionTime > SLOW_QUERY_THRESHOLD_MS) {
    LOGGER.log(Level.WARNING, "Slow query detected: {0} took {1}ms", 
               new Object[]{query, executionTime});
}
```

### 2.3 Health Check Endpoint
**Описание:** Эндпоинт для проверки работоспособности
- Статус сервера (UP/DOWN)
- Доступность диска для записи
- Возможность принимать новые подключения
- Статус основных компонентов

**Зачем:** Интеграция с orchestrator'ами (Kubernetes), load balancer'ами.

**Реализация:** Добавить отдельный порт или команду:
```java
// Новый тип запроса HEALTH_CHECK
// Возвращает JSON: {"status": "UP", "activeConnections": 5, "uptime": 3600}
```

### 2.4 Метрики операций CRUD
**Описание:** Статистика по типам операций
- SELECT count / sec
- INSERT count / sec
- UPDATE count / sec
- DELETE count / sec
- CREATE TABLE / INDEX operations

**Зачем:** Понимание паттернов нагрузки, планирование масштабирования.

**Реализация:**
```java
private final AtomicLong selectCount = new AtomicLong();
private final AtomicLong insertCount = new AtomicLong();
private final AtomicLong updateCount = new AtomicLong();
private final AtomicLong deleteCount = new AtomicLong();
```

### 2.5 Deadlock Detection
**Описание:** Обнаружение и логирование взаимных блокировок
- Количество обнаруженных deadlock'ов
- Участствующие транзакции
- Заблокированные ресурсы

**Зачем:** Критично для систем с высокой конкурентностью.

**Реализация:** Использовать `ThreadMXBean.findDeadlockedThreads()` или собственный детектор на основе графа ожиданий.

---

## Приоритет 3: Средний (Nice to Have)

Эти функции улучшают наблюдаемость и упрощают эксплуатацию.

### 3.1 Экспорт метрик в外部ние системы
**Описание:** Интеграция с системами мониторинга
- Prometheus endpoint (/metrics)
- StatsD client
- JMX exporter
- OpenTelemetry tracing

**Зачем:** Визуализация в Grafana, алертинг в Alertmanager.

**Реализация:** Добавить зависимость micrometer-registry-prometheus:
```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
    <version>1.11.0</version>
</dependency>
```

### 3.2 Аудит действий пользователей
**Описание:** Логирование всех значимых действий
- Кто выполнил запрос (client IP)
- Когда был выполнен
- Какой запрос выполнен
- Результат (успех/ошибка)

**Зачем:** Безопасность, compliance, расследование инцидентов.

**Реализация:** Асинхронный аудит-лог в отдельный файл:
```java
// В ClientHandler.run()
auditLogger.info("Client {} executed query: {}", clientSocket.getInetAddress(), query);
```

### 3.3 Метрики буферного кэша
**Описание:** Если будет реализован buffer pool
- Cache hit ratio
- Количество страниц в кэше
- eviction rate
- Dirty pages count

**Зачем:** Оптимизация размера буфера, оценка эффективности кэширования.

### 3.4 Replication Lag (для будущей репликации)
**Описание:** Отставание реплик от мастера
- Lag в секундах
- Lag в байтах WAL
- Статус каждой реплики

**Зачем:** Контроль консистентности реплик.

### 3.5 Query Plan Statistics
**Описание:** Статистика планов выполнения
- Количество full table scans
- Количество index scans
- Выбранные планы выполнения
- Cost estimates vs actual time

**Зачем:** Оптимизация запросов, настройка индексов.

---

## Приоритет 4: Перспективный (Future)

Функции для зрелой production-системы.

### 4.1 Distributed Tracing
**Описание:** Трассировка запросов через компоненты
- Trace ID для каждого запроса
- Span duration для каждой операции
- Propagation через клиентские приложения

**Интеграция:** Jaeger, Zipkin, OpenTelemetry.

### 4.2 Predictive Analytics
**Описание:** Прогнозирование проблем на основе ML
- Прогноз заполнения диска
- Аномалии в производительности
- Рекомендации по оптимизации

### 4.3 Automatic Tuning Recommendations
**Описание:** Советы по настройке на основе метрик
- Рекомендации по размеру пула соединений
- Настройки isolation level
- Индексы для создания/удаления

### 4.4 Real-time Dashboard
**Описание:** Встроенный веб-интерфейс мониторинга
- Графики ключевых метрик
- Список активных запросов
- Top-10 медленных запросов
- Статус таблиц и индексов

---

## Сводная таблица приоритетов

| Категория | Функция | Приоритет | Сложность | Ценность |
|-----------|---------|-----------|-----------|----------|
| Производительность | Метрики времени запросов | P1 | Низкая | Высокая |
| Подключения | Статистика пула соединений | P1 | Низкая | Высокая |
| Транзакции | Метрики активных транзакций | P1 | Средняя | Высокая |
| Ресурсы | Memory/CPU метрики | P1 | Низкая | Высокая |
| Ошибки | Логирование и счётчики ошибок | P1 | Низкая | Высокая |
| Таблицы | Статистика таблиц и индексов | P2 | Средняя | Средняя |
| Оптимизация | Slow Query Log | P2 | Низкая | Высокая |
| Availability | Health Check Endpoint | P2 | Средняя | Высокая |
| Операции | CRUD метрики | P2 | Низкая | Средняя |
| Надёжность | Deadlock Detection | P2 | Высокая | Высокая |
| Интеграция | Prometheus Export | P3 | Средняя | Средняя |
| Безопасность | Audit Log | P3 | Средняя | Средняя |
| Кэш | Buffer Pool Metrics | P3 | Высокая | Средняя |
| Репликация | Replication Lag | P3 | Высокая | Средняя |
| Оптимизация | Query Plan Stats | P3 | Высокая | Средняя |
| Observability | Distributed Tracing | P4 | Высокая | Низкая |
| AI/ML | Predictive Analytics | P4 | Очень высокая | Низкая |
| Автонастройка | Tuning Recommendations | P4 | Очень высокая | Средняя |
| UI | Real-time Dashboard | P4 | Высокая | Средняя |

---

## Рекомендуемый план внедрения

### Фаза 1 (Недели 1-2): Базовый мониторинг
1. Метрики производительности запросов
2. Метрики подключений
3. Логирование ошибок
4. Slow Query Log

### Фаза 2 (Недели 3-4): Транзакции и ресурсы
1. Метрики транзакций
2. Метрики памяти (JMX)
3. CRUD метрики
4. Health Check Endpoint

### Фаза 3 (Недели 5-6): Продвинутый мониторинг
1. Метрики таблиц и индексов
2. Deadlock Detection
3. Интеграция с Prometheus

### Фаза 4 (Недели 7+): Перспективные функции
1. Audit Log
2. Query Plan Statistics
3. Real-time Dashboard

---

## Пример интеграции с Prometheus

После реализации экспорта метрик, пример scrape config:

```yaml
scrape_configs:
  - job_name: 'dieseldb'
    static_configs:
      - targets: ['localhost:9090']  # порт metrics endpoint
    scrape_interval: 15s
    metrics_path: '/metrics'
```

Пример Grafana dashboard панелей:
- Request rate (queries/sec)
- Query latency (p50, p95, p99)
- Active connections
- Transaction commit/rollback rate
- Memory usage
- Error rate

---

## Заключение

Внедрение мониторинга следует начинать с функций Приоритета 1, так как они дают максимальную отдачу при минимальной сложности реализации. По мере развития DieselDB и перехода к production-использованию, следует постепенно добавлять функции более высоких приоритетов.

Ключевые принципы:
1. **Измеряй всё важное** — если метрику нельзя измерить, ей нельзя управлять
2. **Минимальные накладные расходы** — мониторинг не должен существенно влиять на производительность
3. **Алертинг на основе метрик** — автоматическое уведомление об аномалиях
4. **Документирование** — каждая метрика должна иметь описание и контекст

---

*Документ создан для планирования развития системы мониторинга DieselDB. Актуальность: 2025 год.*

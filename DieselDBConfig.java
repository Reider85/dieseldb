public class DieselDBConfig {
    public static final int PORT = 9090;
    public static final String DATA_DIR = "dieseldb_data";
    public static final long MEMORY_THRESHOLD = 512 * 1024 * 1024; // 512 MB
    public static final long INACTIVE_TIMEOUT = 30_000; // 30 секунд
    public static final int BATCH_SIZE = 1000; // Размер пакета для удаления
    public static final int DISK_FLUSH_INTERVAL_SECONDS = 5; // Интервал записи на диск
    public static final int MEMORY_CLEANUP_INTERVAL_SECONDS = 10; // Интервал очистки памяти
    public static final int MAX_WRITE_WAIT_SECONDS = 5; // Максимальное время ожидания записи
    public static final int MAX_JOIN_RESULTS = 1_000_000; // Максимальное количество строк в результате JOIN
}
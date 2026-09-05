package diesel;

import diesel.format.FormatRegistry;
import diesel.format.TableFormat;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Background job that converts large row-based tables to columnar (Parquet)
 * storage for efficient analytical (OLAP) queries. This implements the
 * "dual storage" architecture of Prompt 88: OLTP operations continue to use
 * the in-memory {@link Table} rows while OLAP queries read from the Parquet
 * file produced by this job.
 *
 * <p>Conversion is triggered when a table's live row count exceeds
 * {@link Table#COLUMNAR_THRESHOLD_ROWS} and no conversion is already in
 * progress. The job runs on a low-priority daemon scheduler so it never
 * blocks OLTP work. A synchronous fallback ({@link #runSynchronous}) is
 * available for queries that arrive before the background job completes.
 *
 * <p>After a DML mutation (INSERT, UPDATE, DELETE), the columnar storage is
 * invalidated by {@link Table#invalidateColumnarStorage()} and will be
 * reconverted on the next eligible scan.
 *
 * @see ColumnarTableStorage
 * @see ParquetWriter
 * @see Table
 */
final class ColumnarConversionJob {

    private static final Logger LOGGER = Logger.getLogger(ColumnarConversionJob.class.getName());

    /** Default scan interval in seconds for the background scheduler. */
    private static final long DEFAULT_SCAN_INTERVAL_SECONDS = 30;

    /** Daemon scheduler shared across all tables. */
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "diesel-columnar-conversion");
                t.setDaemon(true);
                return t;
            });

    private ColumnarConversionJob() {
    }

    /**
     * Schedules periodic scans of all tables to convert eligible ones to
     * columnar storage. Called once at database startup. The initial delay
     * is half the interval so the first scan happens quickly.
     *
     * @param database the database whose tables to scan
     */
    static void schedulePeriodicScans(Database database) {
        long intervalSeconds = loadScanInterval();
        long initialDelay = intervalSeconds / 2;
        try {
            SCHEDULER.scheduleWithFixedDelay(
                    () -> scanAndConvert(database),
                    initialDelay, intervalSeconds, TimeUnit.SECONDS);
            LOGGER.log(Level.INFO, "Columnar conversion job scheduled every {0}s (initial delay {1}s)",
                    new Object[]{intervalSeconds, initialDelay});
        } catch (RejectedExecutionException e) {
            LOGGER.log(Level.WARNING, "Failed to schedule columnar conversion job: {0}", e.getMessage());
        }
    }

    /**
     * Scans all tables in the database and converts eligible ones to
     * columnar storage. Runs on the daemon scheduler thread.
     *
     * @param database the database to scan
     */
    private static void scanAndConvert(Database database) {
        try {
            for (String tableName : database.getTableNames()) {
                Table table;
                try {
                    table = database.getTable(tableName);
                } catch (Exception e) {
                    continue; // table dropped during iteration
                }
                if (table.isEligibleForColumnarConversion()) {
                    convertInBackground(table);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Columnar conversion scan failed: {0}", e.getMessage());
        }
    }

    /**
     * Schedules a single background conversion for the given table.
     *
     * @param table the table to convert
     */
    private static void convertInBackground(Table table) {
        table.setColumnarConversionState(Table.ColumnarConversionState.IN_PROGRESS);
        try {
            SCHEDULER.submit(() -> {
                try {
                    runConversion(table);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Background columnar conversion failed for {0}: {1}",
                            new Object[]{table.getName(), e.getMessage()});
                    table.setColumnarConversionState(Table.ColumnarConversionState.FAILED);
                }
            });
        } catch (RejectedExecutionException e) {
            LOGGER.log(Level.WARNING, "Failed to submit conversion for {0}: {1}",
                    new Object[]{table.getName(), e.getMessage()});
            table.setColumnarConversionState(Table.ColumnarConversionState.FAILED);
        }
    }

    /**
     * Synchronously converts the table to columnar storage. Called by
     * {@link Table#ensureColumnarStorage()} when the background job has
     * not yet completed and a query needs columnar access immediately.
     *
     * @param table the table to convert
     */
    static void runSynchronous(Table table) {
        LOGGER.log(Level.INFO, "Running synchronous columnar conversion for {0}", table.getName());
        try {
            runConversion(table);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Synchronous columnar conversion failed for {0}: {1}",
                    new Object[]{table.getName(), e.getMessage()});
            table.setColumnarConversionState(Table.ColumnarConversionState.FAILED);
            throw new DieselIOException(
                    "Columnar conversion failed for table " + table.getName(), e);
        }
    }

    /**
     * Core conversion logic: writes the table to Parquet via the
     * {@link FormatRegistry} and activates columnar storage on success.
     * Shared by both background and synchronous paths.
     *
     * @param table the table to convert
     */
    private static void runConversion(Table table) {
        Path parquetPath = table.getParquetFilePath();
        LOGGER.log(Level.INFO, "Converting table {0} to columnar storage ({1} rows)",
                new Object[]{table.getName(), table.getLiveRowCount()});

        TableFormat parquetFormat = FormatRegistry.get(ErrorMessages.STORAGE_FORMAT_PARQUET);
        if (parquetFormat == null) {
            throw new IllegalStateException("Parquet format not registered in FormatRegistry");
        }
        try {
            parquetFormat.write(table.toTableData(), parquetPath, diesel.format.WriteOptions.DEFAULT);
        } catch (Exception e) {
            throw new DieselIOException("Failed to write Parquet file for table " + table.getName(), e);
        }

        Map<String, Class<?>> types = table.getColumnTypes();
        ColumnarTableStorage storage = new ColumnarTableStorage(parquetPath, table.getName());
        storage.setColumnTypes(types);
        table.activateColumnarStorage(storage);

        LOGGER.log(Level.INFO, "Columnar conversion completed for {0}: {1}",
                new Object[]{table.getName(), parquetPath});
    }

    /**
     * Loads the scan interval from {@code config.properties}, defaulting
     * to {@link #DEFAULT_SCAN_INTERVAL_SECONDS}.
     *
     * @return the interval in seconds
     */
    private static long loadScanInterval() {
        try {
            java.io.File configFile = new java.io.File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                java.util.Properties props = new java.util.Properties();
                try (java.io.FileInputStream fis = new java.io.FileInputStream(configFile)) {
                    props.load(fis);
                }
                String raw = props.getProperty("columnar.conversion.interval.seconds");
                if (raw != null) {
                    long v = Long.parseLong(raw.trim());
                    if (v > 0) return v;
                }
            }
        } catch (Exception ignored) {
            // Keep default
        }
        return DEFAULT_SCAN_INTERVAL_SECONDS;
    }
}

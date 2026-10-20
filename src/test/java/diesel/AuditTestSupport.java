package diesel;

import diesel.security.AuditLogger;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for the prompt4 #33 audit tests: reaches the package-private
 * {@link Database#getAuditLogger()} hooks and reads the DIESEL_AUDIT_LOG
 * table.
 */
final class AuditTestSupport {

    static final String ENABLED_KEY = AuditLogger.ENABLED_KEY;
    static final String STATUS_OK = "OK";
    static final String STATUS_ERROR = "ERROR";

    private AuditTestSupport() {
    }

    /** Synchronously drains the audit queue on the caller thread. */
    static void flush(Database database) {
        AuditLogger logger = database.getAuditLogger();
        if (logger != null) {
            logger.flushForTest();
        }
    }

    /** Returns every row currently in DIESEL_AUDIT_LOG. */
    static List<Map<String, Object>> auditRows(Database database) {
        return database.getTable(diesel.security.AuditBackend.AUDIT).getRows();
    }
}
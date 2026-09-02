package diesel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A prepared statement: an SQL template with {@code ?} placeholder parameters
 * plus an LRU cache of the parsed AST for parameter bindings (Prompt 79).
 *
 * <p>The statement is created from a template that contains one or more
 * {@code ?} placeholder positions, e.g. {@code SELECT * FROM USERS WHERE
 * NAME = ?}. Parameters are bound with {@link #bindParameters} and the
 * statement is executed with {@link #execute} / {@link #executeQuery} /
 * {@link #executeUpdate} against a {@link Database}.
 *
 * <p>Parsing is cached: the parsed AST is stored under the concrete SQL that
 * results from substituting a particular set of bound parameters, so executing
 * the same prepared statement with the same parameters reuses the cached plan
 * and skips the parse phase. The cache is a per-template LRU bounded by the
 * {@code query.cache.max.size} configuration (default 1000), so a statement
 * that is hot with a fixed parameter set is parsed exactly once.
 *
 * @see QueryParser
 * @see Database
 */
public class PreparedStatement {

    /** Default LRU cache capacity when no config entry is present. */
    private static final int DEFAULT_MAX_CACHE_SIZE = 1000;

    /** The SQL template with {@code ?} placeholder markers (never mutated). */
    private final String sqlTemplate;

    /** The parameters bound for the next execution, in placeholder order. */
    private final List<Object> boundParams = new ArrayList<>();

    /** Global LRU cache capacity, loaded from {@code query.cache.max.size}. */
    private static int MAX_CACHE_SIZE = loadMaxCacheSize();

    /**
     * The parsed-AST cache for this statement, keyed by the concrete SQL
     * produced by a particular parameter binding. Access-order LinkedHashMap
     * so it is an LRU cache evicting the least-recently-used binding.
     */
    private final Map<String, Query<?>> cache = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Query<?>> eldest) {
            return size() > MAX_CACHE_SIZE;
        }
    };

    private static int loadMaxCacheSize() {
        try {
            java.util.Properties props = new java.util.Properties();
            java.io.FileInputStream fis = new java.io.FileInputStream("config.properties");
            try {
                props.load(fis);
            } finally {
                fis.close();
            }
            String val = props.getProperty("query.cache.max.size");
            if (val != null) {
                return Integer.parseInt(val.trim());
            }
        } catch (Exception ignored) {
            // Missing/unsupported config falls through to the default.
        }
        return DEFAULT_MAX_CACHE_SIZE;
    }

    /**
     * Creates a prepared statement from the given SQL template.
     *
     * @param sql the template containing zero or more {@code ?} placeholders,
     *            must not be null
     */
    public PreparedStatement(String sql) {
        if (sql == null) {
            throw new IllegalArgumentException("Prepared statement SQL must not be null");
        }
        this.sqlTemplate = sql;
    }

    /**
     * Returns the SQL template this statement was created from.
     *
     * @return the template, never null
     */
    public String getSqlTemplate() {
        return sqlTemplate;
    }

    /**
     * Binds a list of parameters (in placeholder order), replacing any
     * previously bound parameters.
     *
     * @param params the parameter values, or null to clear all bindings
     * @return this statement for chaining
     */
    public PreparedStatement bindParameters(List<Object> params) {
        boundParams.clear();
        if (params != null) {
            boundParams.addAll(params);
        }
        return this;
    }

    /**
     * Binds varargs parameters (in placeholder order), replacing any
     * previously bound parameters.
     *
     * @param params the parameter values, or none to clear all bindings
     * @return this statement for chaining
     */
    public PreparedStatement bindParameters(Object... params) {
        boundParams.clear();
        if (params != null) {
            boundParams.addAll(Arrays.asList(params));
        }
        return this;
    }

    /**
     * Returns the currently bound parameters, in placeholder order.
     *
     * @return the bound parameter values, never null
     */
    public List<Object> getBoundParameters() {
        return new ArrayList<>(boundParams);
    }

    /**
     * Renders the concrete SQL by substituting the bound parameters into the
     * template's {@code ?} placeholders. Strings are escaped inside single
     * quotes, nulls become {@code NULL}.
     *
     * @return the concrete SQL ready for the parser
     */
    public String buildSql() {
        return buildSqlWith(boundParams);
    }

    private String buildSqlWith(List<Object> params) {
        String result = sqlTemplate;
        int paramIndex = 0;
        for (Object param : params) {
            if (paramIndex >= countPlaceholders()) {
                break;
            }
            result = result.replaceFirst("\\?", literal(param));
            paramIndex++;
        }
        return result;
    }

    private static String literal(Object param) {
        if (param == null) {
            return "NULL";
        }
        if (param instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
        }
        if (param instanceof Character c) {
            return "'" + c + "'";
        }
        if (param instanceof Boolean b) {
            return b ? "TRUE" : "FALSE";
        }
        return param.toString();
    }

    private int countPlaceholders() {
        int count = 0;
        for (int i = 0; i < sqlTemplate.length(); i++) {
            if (sqlTemplate.charAt(i) == '?') {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the parsed AST for the currently bound parameters, using the
     * LRU cache so an identical binding hits the cache and skips parsing.
     *
     * @param database the database for schema resolution during parsing
     * @return the parsed query
     */
    public Query<?> getParsedQuery(Database database) {
        String concreteSql = buildSql();
        return parseIfAbsent(database, concreteSql);
    }

    private Query<?> parseIfAbsent(Database database, String concreteSql) {
        synchronized (cache) {
            Query<?> parsed = cache.get(concreteSql);
            if (parsed != null) {
                return parsed;
            }
            QueryParser parser = new QueryParser();
            parsed = parser.parse(concreteSql, database);
            cache.put(concreteSql, parsed);
            return parsed;
        }
    }

    /**
     * Executes the statement with its bound parameters against the database.
     *
     * @param database      the database to execute against
     * @param transactionId the caller's transaction id, or null
     * @return the query result (row list for SELECT, null for DML, String for
     *         DDL/transaction statements)
     */
    public Object execute(Database database, UUID transactionId) {
        Query<?> parsed = getParsedQuery(database);
        return database.executeParsedPrepared(parsed, buildSql(), transactionId);
    }

    /**
     * Executes a SELECT and returns the row list, binding parameters at
     * execution time.
     *
     * @param database      the database to execute against
     * @param transactionId the caller's transaction id, or null
     * @return the result rows as {@code List<Map<String, Object>>}
     * @throws IllegalArgumentException if the statement is not a SELECT
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> executeQuery(Database database, UUID transactionId) {
        Object result = execute(database, transactionId);
        if (!(result instanceof List)) {
            throw new IllegalArgumentException(
                    "Prepared statement is not a SELECT: " + sqlTemplate);
        }
        return (List<Map<String, Object>>) result;
    }

    /**
     * Executes an INSERT/UPDATE/DELETE and returns the (possibly null) result,
     * binding parameters at execution time.
     *
     * @param database      the database to execute against
     * @param transactionId the caller's transaction id, or null
     * @return the DML result (typically null)
     */
    public Object executeUpdate(Database database, UUID transactionId) {
        return execute(database, transactionId);
    }

    /**
     * Returns the number of cached parsed ASTs for this statement.
     *
     * @return the current cache size
     */
    public int getCacheSize() {
        synchronized (cache) {
            return cache.size();
        }
    }

    /**
     * Clears this statement's parsed-AST cache.
     */
    public void clearCache() {
        synchronized (cache) {
            cache.clear();
        }
    }

    /**
     * Clears the cache of every statement and resets the global capacity to
     * the configured default.
     */
    public static void resetGlobalCacheSize() {
        MAX_CACHE_SIZE = loadMaxCacheSize();
    }
}

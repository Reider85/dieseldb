package diesel;

import java.util.*;

/**
 * Prepared statement with parameter binding and LRU-cached parsed AST.
 * Key: normalized SQL structure (with placeholders). Value: parsed Query.
 */
public class PreparedStatement {
    private final String sqlTemplate;
    private final List<Object> boundParams = new ArrayList<>();

    // LRU cache for parsed AST, maxSize = 1000
    private static int MAX_CACHE_SIZE = 1000;

    static {
        try {
            java.util.Properties props = new java.util.Properties();
            java.io.FileInputStream fis = new java.io.FileInputStream("config.properties");
            props.load(fis);
            fis.close();
            String val = props.getProperty("query.cache.max.size");
            if (val != null) MAX_CACHE_SIZE = Integer.parseInt(val.trim());
        } catch (Exception ignored) {}
    }

    private static final Map<String, Query<?>> cache = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Query<?>> eldest) {
            return size() > MAX_CACHE_SIZE;
        }
    };

    public PreparedStatement(String sql) {
        this.sqlTemplate = sql;
    }

    public PreparedStatement bindParameters(List<Object> params) {
        boundParams.clear();
        if (params != null) {
            boundParams.addAll(params);
        }
        return this;
    }

    public PreparedStatement bindParameters(Object... params) {
        boundParams.clear();
        if (params != null) {
            boundParams.addAll(Arrays.asList(params));
        }
        return this;
    }

    public String buildSql() {
        String result = sqlTemplate;
        int paramIndex = 0;
        for (Object param : boundParams) {
            String replacement = param instanceof String ? "'" + param + "'" : (param == null ? "NULL" : param.toString());
            result = result.replaceFirst("\\?", replacement);
        }
        return result;
    }

    public Query<?> getParsedQuery(Database database) {
        String concreteSql = buildSql();
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

    public int getCacheSize() {
        synchronized (cache) {
            return cache.size();
        }
    }

    public static void clearCache() {
        synchronized (cache) {
            cache.clear();
        }
    }
}

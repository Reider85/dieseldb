package com.diesel.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for the DieselDB libpq server.
 *
 * <p>Connection URL format: {@code jdbc:diesel://host:port/database}
 *
 * <p>Supported connection properties:
 * <ul>
 *   <li>{@code user} - username (default "postgres")</li>
 *   <li>{@code password} - password (default empty; server may use trust auth)</li>
 *   <li>{@code connectTimeout} - connect timeout in seconds (default 10)</li>
 * </ul>
 */
public class DieselDriver implements Driver {

    /** URL prefix recognized by this driver. */
    public static final String URL_PREFIX = "jdbc:diesel://";

    static {
        try {
            java.sql.DriverManager.registerDriver(new DieselDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null; // per JDBC spec: return null for non-matching URL
        }
        try {
            return new DieselConnection(url, info);
        } catch (java.io.IOException e) {
            throw new SQLException("Failed to connect to " + url + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }

    /**
     * Parses {@code jdbc:diesel://host:port/database} into host, port, database.
     * Package-private helper used by {@link DieselConnection}.
     */
    static String[] parseUrl(String url) throws SQLException {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            throw new SQLException("Invalid DieselDB URL: " + url);
        }
        String rest = url.substring(URL_PREFIX.length());
        String host = "localhost";
        int port = 5432;
        String database = "postgres";

        int slash = rest.indexOf('/');
        String hostPort = slash >= 0 ? rest.substring(0, slash) : rest;
        if (slash >= 0 && slash + 1 < rest.length()) {
            database = rest.substring(slash + 1);
            // strip any query string
            int q = database.indexOf('?');
            if (q >= 0) {
                database = database.substring(0, q);
            }
        }

        int colon = hostPort.lastIndexOf(':');
        if (colon >= 0) {
            host = hostPort.substring(0, colon);
            try {
                port = Integer.parseInt(hostPort.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid port in URL: " + url, e);
            }
        } else if (!hostPort.isEmpty()) {
            host = hostPort;
        }

        return new String[]{host, String.valueOf(port), database};
    }
}

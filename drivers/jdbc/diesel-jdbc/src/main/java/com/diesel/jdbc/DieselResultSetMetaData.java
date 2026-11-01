package com.diesel.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * {@link ResultSetMetaData} over the column descriptors of a query result.
 */
public class DieselResultSetMetaData implements ResultSetMetaData {

    private final String[] columnNames;
    private final int[] columnTypeOids;

    DieselResultSetMetaData(String[] columnNames, int[] columnTypeOids) {
        this.columnNames = columnNames;
        this.columnTypeOids = columnTypeOids;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int oid = oid(column);
        return oid == 20 || oid == 21 || oid == 23 || oid == 700 || oid == 701 || oid == 1700;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 64;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumn(column);
        return columnNames[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        int oid = oid(column);
        switch (oid) {
            case 16: return Types.BOOLEAN;
            case 20: return Types.BIGINT;
            case 21: return Types.SMALLINT;
            case 23: return Types.INTEGER;
            case 25: return Types.VARCHAR;
            case 700: return Types.REAL;
            case 701: return Types.DOUBLE;
            case 1043: return Types.VARCHAR;
            case 1082: return Types.DATE;
            case 1083: return Types.TIME;
            case 1114: return Types.TIMESTAMP;
            case 1184: return Types.TIMESTAMP_WITH_TIMEZONE;
            case 1700: return Types.NUMERIC;
            case 2950: return Types.OTHER; // uuid
            default: return Types.VARCHAR;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int oid = oid(column);
        switch (oid) {
            case 16: return "bool";
            case 20: return "int8";
            case 21: return "int2";
            case 23: return "int4";
            case 25: return "text";
            case 700: return "float4";
            case 701: return "float8";
            case 1043: return "varchar";
            case 1082: return "date";
            case 1083: return "time";
            case 1114: return "timestamp";
            case 1184: return "timestamptz";
            case 1700: return "numeric";
            case 2950: return "uuid";
            default: return "text";
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        switch (getColumnType(column)) {
            case Types.BOOLEAN: return "java.lang.Boolean";
            case Types.BIGINT: return "java.lang.Long";
            case Types.SMALLINT: return "java.lang.Short";
            case Types.INTEGER: return "java.lang.Integer";
            case Types.REAL: return "java.lang.Float";
            case Types.DOUBLE: return "java.lang.Double";
            case Types.NUMERIC:
            case Types.DECIMAL: return "java.math.BigDecimal";
            case Types.DATE: return "java.sql.Date";
            case Types.TIME: return "java.sql.Time";
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE: return "java.sql.Timestamp";
            default: return "java.lang.String";
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    private int oid(int column) throws SQLException {
        checkColumn(column);
        return column - 1 < columnTypeOids.length ? columnTypeOids[column - 1] : 0;
    }

    private void checkColumn(int column) throws SQLException {
        if (column < 1 || column > columnNames.length) {
            throw new SQLException("Column index out of range: " + column);
        }
    }
}

package com.diesel.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Minimal {@link ParameterMetaData} reporting the placeholder count of a
 * prepared statement. Parameter types are unknown until execution, so all
 * getters return generic values.
 */
public class DieselParameterMetaData implements ParameterMetaData {

    private final int count;

    DieselParameterMetaData(int count) {
        this.count = count;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return count;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return "OTHER";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return "java.lang.Object";
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeIn;
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
}

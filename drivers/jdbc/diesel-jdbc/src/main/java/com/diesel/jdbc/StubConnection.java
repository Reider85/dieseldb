package com.diesel.jdbc;

import java.sql.*;
import java.util.*;
import java.util.Calendar;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;

/**
 * Generated stub implementation of {@link Connection}.
 * Methods throw SQLFeatureNotSupportedException unless overridden.
 */
public class StubConnection implements Connection {

    @Override public java.sql.Statement createStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException("createStatement");
    }

    @Override public java.sql.PreparedStatement prepareStatement(java.lang.String p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override public java.sql.CallableStatement prepareCall(java.lang.String p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override public java.lang.String nativeSQL(java.lang.String p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("nativeSQL");
    }

    @Override public void setAutoCommit(boolean p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAutoCommit");
    }

    @Override public boolean getAutoCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException("getAutoCommit");
    }

    @Override public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("commit");
    }

    @Override public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback");
    }

    @Override public void close() throws SQLException {
        throw new SQLFeatureNotSupportedException("close");
    }

    @Override public boolean isClosed() throws SQLException {
        throw new SQLFeatureNotSupportedException("isClosed");
    }

    @Override public java.sql.DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMetaData");
    }

    @Override public void setReadOnly(boolean p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setReadOnly");
    }

    @Override public boolean isReadOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException("isReadOnly");
    }

    @Override public void setCatalog(java.lang.String p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCatalog");
    }

    @Override public java.lang.String getCatalog() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCatalog");
    }

    @Override public void setTransactionIsolation(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTransactionIsolation");
    }

    @Override public int getTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTransactionIsolation");
    }

    @Override public java.sql.SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("getWarnings");
    }

    @Override public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("clearWarnings");
    }

    @Override public java.sql.Statement createStatement(int p0, int p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStatement");
    }

    @Override public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int p1, int p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override public java.sql.CallableStatement prepareCall(java.lang.String p0, int p1, int p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override public java.util.Map<java.lang.String, java.lang.Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap");
    }

    @Override public void setTypeMap(java.util.Map<java.lang.String,java.lang.Class<?>> p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap");
    }

    @Override public void setHoldability(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setHoldability");
    }

    @Override public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getHoldability");
    }

    @Override public java.sql.Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    @Override public java.sql.Savepoint setSavepoint(java.lang.String p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    @Override public void rollback(java.sql.Savepoint p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback");
    }

    @Override public void releaseSavepoint(java.sql.Savepoint p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint");
    }

    @Override public java.sql.Statement createStatement(int p0, int p1, int p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStatement");
    }

    @Override public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int p1, int p2, int p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override public java.sql.CallableStatement prepareCall(java.lang.String p0, int p1, int p2, int p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override public java.sql.PreparedStatement prepareStatement(java.lang.String p0, int[] p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override public java.sql.PreparedStatement prepareStatement(java.lang.String p0, java.lang.String[] p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override public java.sql.Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob");
    }

    @Override public java.sql.Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob");
    }

    @Override public java.sql.NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    @Override public java.sql.SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML");
    }

    @Override public boolean isValid(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("isValid");
    }

    @Override public void setClientInfo(java.lang.String p0, java.lang.String p1) {
        throw new UnsupportedOperationException("setClientInfo");
    }

    @Override public void setClientInfo(java.util.Properties p0) {
        throw new UnsupportedOperationException("setClientInfo");
    }

    @Override public java.lang.String getClientInfo(java.lang.String p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo");
    }

    @Override public java.util.Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo");
    }

    @Override public java.sql.Array createArrayOf(java.lang.String p0, java.lang.Object[] p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    @Override public java.sql.Struct createStruct(java.lang.String p0, java.lang.Object[] p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    @Override public void setSchema(java.lang.String p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSchema");
    }

    @Override public java.lang.String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSchema");
    }

    @Override public void abort(java.util.concurrent.Executor p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("abort");
    }

    @Override public void setNetworkTimeout(java.util.concurrent.Executor p0, int p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNetworkTimeout");
    }

    @Override public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNetworkTimeout");
    }

    @Override public <T> T unwrap(java.lang.Class<T> p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("unwrap");
    }

    @Override public boolean isWrapperFor(java.lang.Class<?> p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("isWrapperFor");
    }

}
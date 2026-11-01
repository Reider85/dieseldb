package com.diesel.jdbc;

import java.sql.*;
import java.util.*;
import java.util.Calendar;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;

/**
 * Generated stub implementation of {@link DatabaseMetaData}.
 * Methods throw SQLFeatureNotSupportedException unless overridden.
 */
public class StubDatabaseMetaData implements DatabaseMetaData {

    @Override public boolean allProceduresAreCallable() throws SQLException {
        throw new SQLFeatureNotSupportedException("allProceduresAreCallable");
    }

    @Override public boolean allTablesAreSelectable() throws SQLException {
        throw new SQLFeatureNotSupportedException("allTablesAreSelectable");
    }

    @Override public java.lang.String getURL() throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override public java.lang.String getUserName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getUserName");
    }

    @Override public boolean isReadOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException("isReadOnly");
    }

    @Override public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLFeatureNotSupportedException("nullsAreSortedHigh");
    }

    @Override public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLFeatureNotSupportedException("nullsAreSortedLow");
    }

    @Override public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException("nullsAreSortedAtStart");
    }

    @Override public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLFeatureNotSupportedException("nullsAreSortedAtEnd");
    }

    @Override public java.lang.String getDatabaseProductName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getDatabaseProductName");
    }

    @Override public java.lang.String getDatabaseProductVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException("getDatabaseProductVersion");
    }

    @Override public java.lang.String getDriverName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getDriverName");
    }

    @Override public java.lang.String getDriverVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException("getDriverVersion");
    }

    @Override public int getDriverMajorVersion() {
        throw new UnsupportedOperationException("getDriverMajorVersion");
    }

    @Override public int getDriverMinorVersion() {
        throw new UnsupportedOperationException("getDriverMinorVersion");
    }

    @Override public boolean usesLocalFiles() throws SQLException {
        throw new SQLFeatureNotSupportedException("usesLocalFiles");
    }

    @Override public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLFeatureNotSupportedException("usesLocalFilePerTable");
    }

    @Override public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsMixedCaseIdentifiers");
    }

    @Override public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("storesUpperCaseIdentifiers");
    }

    @Override public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("storesLowerCaseIdentifiers");
    }

    @Override public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("storesMixedCaseIdentifiers");
    }

    @Override public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsMixedCaseQuotedIdentifiers");
    }

    @Override public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("storesUpperCaseQuotedIdentifiers");
    }

    @Override public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("storesLowerCaseQuotedIdentifiers");
    }

    @Override public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLFeatureNotSupportedException("storesMixedCaseQuotedIdentifiers");
    }

    @Override public java.lang.String getIdentifierQuoteString() throws SQLException {
        throw new SQLFeatureNotSupportedException("getIdentifierQuoteString");
    }

    @Override public java.lang.String getSQLKeywords() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLKeywords");
    }

    @Override public java.lang.String getNumericFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNumericFunctions");
    }

    @Override public java.lang.String getStringFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("getStringFunctions");
    }

    @Override public java.lang.String getSystemFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSystemFunctions");
    }

    @Override public java.lang.String getTimeDateFunctions() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimeDateFunctions");
    }

    @Override public java.lang.String getSearchStringEscape() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSearchStringEscape");
    }

    @Override public java.lang.String getExtraNameCharacters() throws SQLException {
        throw new SQLFeatureNotSupportedException("getExtraNameCharacters");
    }

    @Override public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsAlterTableWithAddColumn");
    }

    @Override public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsAlterTableWithDropColumn");
    }

    @Override public boolean supportsColumnAliasing() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsColumnAliasing");
    }

    @Override public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLFeatureNotSupportedException("nullPlusNonNullIsNull");
    }

    @Override public boolean supportsConvert() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsConvert");
    }

    @Override public boolean supportsConvert(int p0, int p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsConvert");
    }

    @Override public boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsTableCorrelationNames");
    }

    @Override public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsDifferentTableCorrelationNames");
    }

    @Override public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsExpressionsInOrderBy");
    }

    @Override public boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsOrderByUnrelated");
    }

    @Override public boolean supportsGroupBy() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsGroupBy");
    }

    @Override public boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsGroupByUnrelated");
    }

    @Override public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsGroupByBeyondSelect");
    }

    @Override public boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsLikeEscapeClause");
    }

    @Override public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsMultipleResultSets");
    }

    @Override public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsMultipleTransactions");
    }

    @Override public boolean supportsNonNullableColumns() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsNonNullableColumns");
    }

    @Override public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsMinimumSQLGrammar");
    }

    @Override public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsCoreSQLGrammar");
    }

    @Override public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsExtendedSQLGrammar");
    }

    @Override public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsANSI92EntryLevelSQL");
    }

    @Override public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsANSI92IntermediateSQL");
    }

    @Override public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsANSI92FullSQL");
    }

    @Override public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsIntegrityEnhancementFacility");
    }

    @Override public boolean supportsOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsOuterJoins");
    }

    @Override public boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsFullOuterJoins");
    }

    @Override public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsLimitedOuterJoins");
    }

    @Override public java.lang.String getSchemaTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSchemaTerm");
    }

    @Override public java.lang.String getProcedureTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException("getProcedureTerm");
    }

    @Override public java.lang.String getCatalogTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCatalogTerm");
    }

    @Override public boolean isCatalogAtStart() throws SQLException {
        throw new SQLFeatureNotSupportedException("isCatalogAtStart");
    }

    @Override public java.lang.String getCatalogSeparator() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCatalogSeparator");
    }

    @Override public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSchemasInDataManipulation");
    }

    @Override public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSchemasInProcedureCalls");
    }

    @Override public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSchemasInTableDefinitions");
    }

    @Override public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSchemasInIndexDefinitions");
    }

    @Override public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSchemasInPrivilegeDefinitions");
    }

    @Override public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsCatalogsInDataManipulation");
    }

    @Override public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsCatalogsInProcedureCalls");
    }

    @Override public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsCatalogsInTableDefinitions");
    }

    @Override public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsCatalogsInIndexDefinitions");
    }

    @Override public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsCatalogsInPrivilegeDefinitions");
    }

    @Override public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsPositionedDelete");
    }

    @Override public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsPositionedUpdate");
    }

    @Override public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSelectForUpdate");
    }

    @Override public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsStoredProcedures");
    }

    @Override public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSubqueriesInComparisons");
    }

    @Override public boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSubqueriesInExists");
    }

    @Override public boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSubqueriesInIns");
    }

    @Override public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSubqueriesInQuantifieds");
    }

    @Override public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsCorrelatedSubqueries");
    }

    @Override public boolean supportsUnion() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsUnion");
    }

    @Override public boolean supportsUnionAll() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsUnionAll");
    }

    @Override public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsOpenCursorsAcrossCommit");
    }

    @Override public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsOpenCursorsAcrossRollback");
    }

    @Override public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsOpenStatementsAcrossCommit");
    }

    @Override public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsOpenStatementsAcrossRollback");
    }

    @Override public int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxBinaryLiteralLength");
    }

    @Override public int getMaxCharLiteralLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxCharLiteralLength");
    }

    @Override public int getMaxColumnNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxColumnNameLength");
    }

    @Override public int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxColumnsInGroupBy");
    }

    @Override public int getMaxColumnsInIndex() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxColumnsInIndex");
    }

    @Override public int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxColumnsInOrderBy");
    }

    @Override public int getMaxColumnsInSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxColumnsInSelect");
    }

    @Override public int getMaxColumnsInTable() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxColumnsInTable");
    }

    @Override public int getMaxConnections() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxConnections");
    }

    @Override public int getMaxCursorNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxCursorNameLength");
    }

    @Override public int getMaxIndexLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxIndexLength");
    }

    @Override public int getMaxSchemaNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxSchemaNameLength");
    }

    @Override public int getMaxProcedureNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxProcedureNameLength");
    }

    @Override public int getMaxCatalogNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxCatalogNameLength");
    }

    @Override public int getMaxRowSize() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxRowSize");
    }

    @Override public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLFeatureNotSupportedException("doesMaxRowSizeIncludeBlobs");
    }

    @Override public int getMaxStatementLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxStatementLength");
    }

    @Override public int getMaxStatements() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxStatements");
    }

    @Override public int getMaxTableNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxTableNameLength");
    }

    @Override public int getMaxTablesInSelect() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxTablesInSelect");
    }

    @Override public int getMaxUserNameLength() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMaxUserNameLength");
    }

    @Override public int getDefaultTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException("getDefaultTransactionIsolation");
    }

    @Override public boolean supportsTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsTransactions");
    }

    @Override public boolean supportsTransactionIsolationLevel(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsTransactionIsolationLevel");
    }

    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsDataDefinitionAndDataManipulationTransactions");
    }

    @Override public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsDataManipulationTransactionsOnly");
    }

    @Override public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLFeatureNotSupportedException("dataDefinitionCausesTransactionCommit");
    }

    @Override public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLFeatureNotSupportedException("dataDefinitionIgnoredInTransactions");
    }

    @Override public java.sql.ResultSet getProcedures(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getProcedures");
    }

    @Override public java.sql.ResultSet getProcedureColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getProcedureColumns");
    }

    @Override public java.sql.ResultSet getTables(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String[] p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTables");
    }

    @Override public java.sql.ResultSet getSchemas() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSchemas");
    }

    @Override public java.sql.ResultSet getCatalogs() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCatalogs");
    }

    @Override public java.sql.ResultSet getTableTypes() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTableTypes");
    }

    @Override public java.sql.ResultSet getColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getColumns");
    }

    @Override public java.sql.ResultSet getColumnPrivileges(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getColumnPrivileges");
    }

    @Override public java.sql.ResultSet getTablePrivileges(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTablePrivileges");
    }

    @Override public java.sql.ResultSet getBestRowIdentifier(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3, boolean p4) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBestRowIdentifier");
    }

    @Override public java.sql.ResultSet getVersionColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getVersionColumns");
    }

    @Override public java.sql.ResultSet getPrimaryKeys(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getPrimaryKeys");
    }

    @Override public java.sql.ResultSet getImportedKeys(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getImportedKeys");
    }

    @Override public java.sql.ResultSet getExportedKeys(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getExportedKeys");
    }

    @Override public java.sql.ResultSet getCrossReference(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, java.lang.String p4, java.lang.String p5) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCrossReference");
    }

    @Override public java.sql.ResultSet getTypeInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeInfo");
    }

    @Override public java.sql.ResultSet getIndexInfo(java.lang.String p0, java.lang.String p1, java.lang.String p2, boolean p3, boolean p4) throws SQLException {
        throw new SQLFeatureNotSupportedException("getIndexInfo");
    }

    @Override public boolean supportsResultSetType(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsResultSetType");
    }

    @Override public boolean supportsResultSetConcurrency(int p0, int p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsResultSetConcurrency");
    }

    @Override public boolean ownUpdatesAreVisible(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("ownUpdatesAreVisible");
    }

    @Override public boolean ownDeletesAreVisible(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("ownDeletesAreVisible");
    }

    @Override public boolean ownInsertsAreVisible(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("ownInsertsAreVisible");
    }

    @Override public boolean othersUpdatesAreVisible(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("othersUpdatesAreVisible");
    }

    @Override public boolean othersDeletesAreVisible(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("othersDeletesAreVisible");
    }

    @Override public boolean othersInsertsAreVisible(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("othersInsertsAreVisible");
    }

    @Override public boolean updatesAreDetected(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("updatesAreDetected");
    }

    @Override public boolean deletesAreDetected(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("deletesAreDetected");
    }

    @Override public boolean insertsAreDetected(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("insertsAreDetected");
    }

    @Override public boolean supportsBatchUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsBatchUpdates");
    }

    @Override public java.sql.ResultSet getUDTs(java.lang.String p0, java.lang.String p1, java.lang.String p2, int[] p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUDTs");
    }

    @Override public java.sql.Connection getConnection() throws SQLException {
        throw new SQLFeatureNotSupportedException("getConnection");
    }

    @Override public boolean supportsSavepoints() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsSavepoints");
    }

    @Override public boolean supportsNamedParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsNamedParameters");
    }

    @Override public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsMultipleOpenResults");
    }

    @Override public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsGetGeneratedKeys");
    }

    @Override public java.sql.ResultSet getSuperTypes(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTypes");
    }

    @Override public java.sql.ResultSet getSuperTables(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTables");
    }

    @Override public java.sql.ResultSet getAttributes(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAttributes");
    }

    @Override public boolean supportsResultSetHoldability(int p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsResultSetHoldability");
    }

    @Override public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSetHoldability");
    }

    @Override public int getDatabaseMajorVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException("getDatabaseMajorVersion");
    }

    @Override public int getDatabaseMinorVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException("getDatabaseMinorVersion");
    }

    @Override public int getJDBCMajorVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException("getJDBCMajorVersion");
    }

    @Override public int getJDBCMinorVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException("getJDBCMinorVersion");
    }

    @Override public int getSQLStateType() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLStateType");
    }

    @Override public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLFeatureNotSupportedException("locatorsUpdateCopy");
    }

    @Override public boolean supportsStatementPooling() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsStatementPooling");
    }

    @Override public java.sql.RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowIdLifetime");
    }

    @Override public java.sql.ResultSet getSchemas(java.lang.String p0, java.lang.String p1) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSchemas");
    }

    @Override public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLFeatureNotSupportedException("supportsStoredFunctionsUsingCallSyntax");
    }

    @Override public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException("autoCommitFailureClosesAllResultSets");
    }

    @Override public java.sql.ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfoProperties");
    }

    @Override public java.sql.ResultSet getFunctions(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFunctions");
    }

    @Override public java.sql.ResultSet getFunctionColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFunctionColumns");
    }

    @Override public java.sql.ResultSet getPseudoColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws SQLException {
        throw new SQLFeatureNotSupportedException("getPseudoColumns");
    }

    @Override public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLFeatureNotSupportedException("generatedKeyAlwaysReturned");
    }

    @Override public <T> T unwrap(java.lang.Class<T> p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("unwrap");
    }

    @Override public boolean isWrapperFor(java.lang.Class<?> p0) throws SQLException {
        throw new SQLFeatureNotSupportedException("isWrapperFor");
    }

}
package com.diesel.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link DatabaseMetaData} for the DieselDB libpq driver.
 *
 * <p>Implements the subset of capabilities Hibernate queries during session
 * bootstrap and simple entity CRUD. Limits and feature flags are conservative
 * but sufficient for Hibernate 6's dialect negotiation.
 */
public class DieselDatabaseMetaData extends StubDatabaseMetaData {

    private final DieselConnection connection;

    DieselDatabaseMetaData(DieselConnection connection) {
        this.connection = connection;
    }

    // ---- product identity ----

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "DieselDB";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "0.1.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "DieselDB JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "0.1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 1;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return "postgres";
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    // ---- SQL feature flags (PostgreSQL-like defaults) ----

    @Override
    public boolean allProceduresAreCallable() throws SQLException { return false; }
    @Override
    public boolean allTablesAreSelectable() throws SQLException { return true; }
    @Override
    public boolean usesLocalFiles() throws SQLException { return false; }
    @Override
    public boolean usesLocalFilePerTable() throws SQLException { return false; }
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException { return true; }
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException { return false; }
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException { return true; }
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException { return false; }
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException { return true; }

    @Override
    public String getIdentifierQuoteString() throws SQLException { return "\""; }
    @Override
    public String getSQLKeywords() throws SQLException {
        return "LIMIT,OFFSET,RETURNING,ILIKE,SIMILAR";
    }
    @Override
    public String getNumericFunctions() throws SQLException { return ""; }
    @Override
    public String getStringFunctions() throws SQLException { return ""; }
    @Override
    public String getSystemFunctions() throws SQLException { return ""; }
    @Override
    public String getTimeDateFunctions() throws SQLException { return ""; }
    @Override
    public String getSearchStringEscape() throws SQLException { return "\\"; }
    @Override
    public String getExtraNameCharacters() throws SQLException { return ""; }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException { return true; }
    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException { return true; }
    @Override
    public boolean supportsColumnAliasing() throws SQLException { return true; }
    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException { return true; }
    @Override
    public boolean supportsConvert() throws SQLException { return false; }
    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException { return false; }
    @Override
    public boolean supportsTableCorrelationNames() throws SQLException { return true; }
    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException { return false; }
    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException { return true; }
    @Override
    public boolean supportsOrderByUnrelated() throws SQLException { return true; }
    @Override
    public boolean supportsGroupBy() throws SQLException { return true; }
    @Override
    public boolean supportsGroupByUnrelated() throws SQLException { return true; }
    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException { return true; }
    @Override
    public boolean supportsLikeEscapeClause() throws SQLException { return true; }
    @Override
    public boolean supportsMultipleResultSets() throws SQLException { return false; }
    @Override
    public boolean supportsMultipleTransactions() throws SQLException { return true; }
    @Override
    public boolean supportsNonNullableColumns() throws SQLException { return true; }
    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException { return true; }
    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException { return true; }
    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException { return false; }
    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException { return true; }
    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException { return false; }
    @Override
    public boolean supportsANSI92FullSQL() throws SQLException { return false; }
    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException { return false; }
    @Override
    public boolean supportsOuterJoins() throws SQLException { return true; }
    @Override
    public boolean supportsFullOuterJoins() throws SQLException { return false; }
    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException { return true; }

    @Override
    public String getSchemaTerm() throws SQLException { return "schema"; }
    @Override
    public String getProcedureTerm() throws SQLException { return "procedure"; }
    @Override
    public String getCatalogTerm() throws SQLException { return "catalog"; }
    @Override
    public boolean isCatalogAtStart() throws SQLException { return true; }
    @Override
    public String getCatalogSeparator() throws SQLException { return "."; }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException { return false; }
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException { return false; }

    @Override
    public boolean supportsPositionedDelete() throws SQLException { return false; }
    @Override
    public boolean supportsPositionedUpdate() throws SQLException { return false; }
    @Override
    public boolean supportsSelectForUpdate() throws SQLException { return false; }
    @Override
    public boolean supportsStoredProcedures() throws SQLException { return false; }
    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException { return true; }
    @Override
    public boolean supportsSubqueriesInExists() throws SQLException { return true; }
    @Override
    public boolean supportsSubqueriesInIns() throws SQLException { return true; }
    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException { return true; }
    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException { return true; }
    @Override
    public boolean supportsUnion() throws SQLException { return true; }
    @Override
    public boolean supportsUnionAll() throws SQLException { return true; }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException { return false; }
    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException { return false; }
    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException { return false; }
    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException { return false; }

    // ---- limits (0 = no limit / unknown) ----

    @Override public int getMaxBinaryLiteralLength() throws SQLException { return 0; }
    @Override public int getMaxCharLiteralLength() throws SQLException { return 0; }
    @Override public int getMaxColumnNameLength() throws SQLException { return 63; }
    @Override public int getMaxColumnsInGroupBy() throws SQLException { return 0; }
    @Override public int getMaxColumnsInIndex() throws SQLException { return 0; }
    @Override public int getMaxColumnsInOrderBy() throws SQLException { return 0; }
    @Override public int getMaxColumnsInSelect() throws SQLException { return 0; }
    @Override public int getMaxColumnsInTable() throws SQLException { return 0; }
    @Override public int getMaxConnections() throws SQLException { return 0; }
    @Override public int getMaxCursorNameLength() throws SQLException { return 63; }
    @Override public int getMaxIndexLength() throws SQLException { return 0; }
    @Override public int getMaxSchemaNameLength() throws SQLException { return 63; }
    @Override public int getMaxProcedureNameLength() throws SQLException { return 63; }
    @Override public int getMaxCatalogNameLength() throws SQLException { return 63; }
    @Override public int getMaxTableNameLength() throws SQLException { return 63; }
    @Override public int getMaxTablesInSelect() throws SQLException { return 0; }
    @Override public int getMaxUserNameLength() throws SQLException { return 63; }

    // ---- transactions ----

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsTransactions() throws SQLException { return true; }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        switch (level) {
            case Connection.TRANSACTION_NONE:
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException { return true; }
    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException { return false; }
    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException { return false; }
    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException { return false; }

    // ---- result sets / updates ----

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException { return false; }
    @Override
    public boolean updatesAreDetected(int type) throws SQLException { return false; }
    @Override
    public boolean deletesAreDetected(int type) throws SQLException { return false; }
    @Override
    public boolean insertsAreDetected(int type) throws SQLException { return false; }
    @Override
    public boolean supportsBatchUpdates() throws SQLException { return false; }

    @Override
    public boolean supportsSavepoints() throws SQLException { return false; }
    @Override
    public boolean supportsNamedParameters() throws SQLException { return false; }
    @Override
    public boolean supportsMultipleOpenResults() throws SQLException { return false; }
    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException { return false; }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException { return false; }
    @Override
    public boolean supportsStatementPooling() throws SQLException { return false; }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException { return false; }
    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException { return false; }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException { return false; }

    // ---- catalog / schema retrieval (empty result sets) ----

    @Override
    public ResultSet getSchemas() throws SQLException {
        return emptyResult("schema_name");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return emptyResult("schema_name");
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return emptyResult("table_cat");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return emptyResult("TABLE_TYPE");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return emptyResult("TYPE_NAME");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern, String columnNamePattern) throws SQLException {
        return emptyResult("COLUMN_NAME");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResult("COLUMN_NAME");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResult("FKCOLUMN_NAME");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResult("PKCOLUMN_NAME");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return emptyResult("FKCOLUMN_NAME");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate) throws SQLException {
        return emptyResult("COLUMN_NAME");
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return emptyResult("PROCEDURE_NAME");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern) throws SQLException {
        return emptyResult("COLUMN_NAME");
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return emptyResult("TYPE_NAME");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptyResult("TYPE_NAME");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResult("TABLE_NAME");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return emptyResult("ATTR_NAME");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResult("NAME");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return emptyResult("FUNCTION_NAME");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern, String columnNamePattern) throws SQLException {
        return emptyResult("COLUMN_NAME");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                      String tableNamePattern, String columnNamePattern) throws SQLException {
        return emptyResult("COLUMN_NAME");
    }

    /**
     * Builds an empty single-column ResultSet locally (no server round-trip):
     * DieselDB's SQL dialect rejects FROM-less SELECTs, and Hibernate only
     * iterates these metadata result sets for rows that never arrive.
     */
    private ResultSet emptyResult(String columnName) throws SQLException {
        connection.checkClosed();
        LibpqWireClient.QueryResult r = new LibpqWireClient.QueryResult(
                new String[]{columnName}, new int[]{25},
                new ArrayList<>(), "SELECT 0", null);
        return new DieselResultSet(null, r);
    }
}

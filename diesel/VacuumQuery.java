package diesel;

/**
 * Executes a {@code VACUUM} statement: reclaims dead row versions through the
 * database's {@link VacuumManager} (prompt4.md step 3).
 *
 * <p>Supported forms:
 * <ul>
 *   <li>{@code VACUUM} — vacuum every registered table</li>
 *   <li>{@code VACUUM <table>} / {@code VACUUM TABLE <table>} — one table</li>
 * </ul>
 *
 * <p>The statement is dispatched in {@code Database.executeVacuum()} before
 * the generic data-query path (the table-name extractor does not know
 * VACUUM); {@link #execute(Table)} is the single-table fallback used when a
 * concrete table instance is supplied directly.
 *
 * @see VacuumManager
 */
class VacuumQuery implements Query<String> {
    private final String tableName;

    /**
     * Creates a VACUUM query.
     *
     * @param tableName the table to vacuum, or {@code null} for all tables
     */
    public VacuumQuery(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Returns the table to vacuum, or {@code null} when the statement vacuums
     * every table.
     *
     * @return the table name, or null
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Vacuums the given table and returns the status message.
     *
     * @param table the table the query operates on
     * @return the VACUUM status message
     */
    @Override
    public String execute(Table table) {
        return table.getDatabase().getVacuumManager().vacuumTable(table);
    }
}

package diesel.schema;

/**
 * Algorithm hint for ALTER TABLE ... ALGORITHM=&lt;name&gt; (R3-010).
 *
 * <p>COPY is implemented in prompt4 #38 (step 2/3): the table is rewritten in
 * the background and swapped in atomically. INPLACE is implemented in
 * prompt4 #39 (step 3/3): metadata-only for ADD COLUMN, tombstone + LOCK
 * gates for DROP COLUMN. DEFAULT keeps the step-1 behavior: metadata-only,
 * instant.
 *
 * @see diesel.AlterTableAddColumnQuery
 * @see diesel.AlterTableDropColumnQuery
 */
public enum AlterAlgorithm {
    /** No clause given: metadata-only schema change (R3-010 step 1). */
    DEFAULT,
    /** Background copy of the whole table, then atomic swap (step 2). */
    COPY,
    /** Metadata-only / tombstone execution with LOCK hints (step 3). */
    INPLACE
}

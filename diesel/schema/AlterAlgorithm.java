package diesel.schema;

/**
 * Algorithm hint for ALTER TABLE ... ALGORITHM=&lt;name&gt; (R3-010).
 *
 * <p>COPY is implemented in prompt4 #38 (step 2/3): the table is rewritten in
 * the background and swapped in atomically. INPLACE is implemented in step 3/3
 * (prompt4 #39) and rejected until then. DEFAULT keeps the step-1 behavior:
 * metadata-only, instant.
 *
 * @see diesel.AlterTableAddColumnQuery
 */
public enum AlterAlgorithm {
    /** No clause given: metadata-only schema change (R3-010 step 1). */
    DEFAULT,
    /** Background copy of the whole table, then atomic swap (step 2). */
    COPY,
    /** Metadata-only / tombstone rewrite with LOCK hints (step 3, not yet executed). */
    INPLACE
}

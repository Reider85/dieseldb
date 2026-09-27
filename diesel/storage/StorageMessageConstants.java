package diesel.storage;

/**
 * Prompt 14 (java:S1192): String literals repeated 3+ times inside a single
 * storage class, extracted into file-shared constants.
 *
 * <p>Each group below was a message fragment repeated inside one class, where
 * the surrounding code assembled the same sentence in several places. Grouping
 * the fragments keeps the wording identical across those call sites and makes
 * an intentional wording change a single edit instead of a sweep.
 *
 * <p>Behaviour is identical to using the inline literal.
 *
 * @since Prompt 14
 */
public final class StorageMessageConstants {

    private StorageMessageConstants() {
    }

    // ---- JSONL row reader (diesel/storage/JsonlRowReader.java) --------------------

    /** Thrown by the iterator once the last record of a JSONL file is consumed. */
    public static final String JSONL_NO_MORE_ROWS = "No more rows in JSONL file";

    /** Appended when a record ends mid-object at physical end of file. */
    public static final String JSONL_TRUNCATED_RECORD =
            " (possibly truncated record: JSON ends unexpectedly at end of file)";

    /** Prefix of the parse error raised when a field name is expected. */
    public static final String JSONL_EXPECTED_FIELD_NAME =
            ": malformed JSON record: expected a field name, found ";

    /** Prefix of the parse error raised when a field value is missing. */
    public static final String JSONL_MISSING_VALUE_FOR_FIELD =
            ": malformed JSON record: missing value for field '";

    // ---- JSONL schema manager (diesel/storage/JsonlSchemaManager.java) ------------

    /** Prefix of the type-mismatch message naming the offending field. */
    public static final String FIELD_PREFIX = "field '";

    /** Suffix of the type-mismatch message naming the target column type. */
    public static final String CANNOT_BE_STORED_IN_COLUMN_TYPE =
            " cannot be stored in column type ";

    // ---- Delimited (CSV/TSV) reader (diesel/storage/DelimitedByteParser.java) -----

    /** Prefix of every malformed-field diagnostic in a delimited file. */
    public static final String MALFORMED = "Malformed ";

    /** Suffix locating a malformed field inside a delimited file. */
    public static final String INPUT_IN_DELIMITED_FILE = " input in delimited file ";

    /** Prefix locating a diagnostic by 1-based line number. */
    public static final String LINE_PREFIX = "line ";

    /** Prefix naming a JSON field in a type-mismatch or inference diagnostic. */
    public static final String JSON_FIELD_PREFIX = "field '";

    // ---- JSON schema inference (diesel/storage/json/JsonSchemaInference.java) -----

    /** Suffix explaining that a silent type conversion was refused. */
    public static final String REFUSES_SILENT_CONVERSION =
            ") - inference refuses a silent conversion";

    // ---- AVRO index managers -------------------------------------------------------

    /** Prefix of the identifier of a secondary index in diagnostics. */
    public static final String INDEX_PREFIX = "Index '";

    /** Prefix of the range-index lookup diagnostic. */
    public static final String RANGE_INDEX_PREFIX = "Range index ";

    /** Suffix of the range-index lookup diagnostic. */
    public static final String NOT_FOUND_SUFFIX = " not found";

    /** Prefix of the diagnostic raised when a schema is not an Avro ENUM. */
    public static final String SCHEMA_NOT_ENUM = "Schema must be an ENUM, got: ";

    /** Prefix naming a field of the reader schema in a schema-conflict message. */
    public static final String READER_FIELD_PREFIX = "Reader field '";

    // ---- AVRO adaptive compression ------------------------------------------------

    /** Content profile reported when a sample mixes compressible and incompressible data. */
    public static final String CONTENT_MIXED = "mixed";

    // ---- AVRO transaction manager -------------------------------------------------

    /** Thrown when an operation is attempted outside a transaction. */
    public static final String NO_ACTIVE_TRANSACTION = "No active transaction: ";

    // ---- AVRO BZip2 codec ---------------------------------------------------------

    /** Prefix of the block-size validation diagnostic. */
    public static final String BLOCK_SIZE_PREFIX = "Block size ";
}

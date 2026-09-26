package diesel.storage.avro;

/**
 * Prompt 14 (java:S1192): String literals duplicated across the AVRO storage
 * module, extracted into named constants.
 *
 * <p>The four Avro container readers - {@link AvroDataFileReader},
 * {@link AvroIntegrityChecker}, {@link AvroInputFormatCompat},
 * {@link AvroParallelReader} and {@link AvroSyncMarkerManager} - all parse the
 * same container format and therefore produced byte-identical error messages
 * and property names. The backup/restore pair
 * ({@link AvroBackupManager} / {@link AvroRestoreManager}) likewise shared the
 * manifest file name, and four schema classes shared one system-property key.
 *
 * <p>Behaviour is identical to using the inline literal; this class only
 * centralises the text so a message cannot drift between readers.
 *
 * @since Prompt 14
 */
final class AvroFileConstants {

    // ---- Container-format error message prefixes --------------------------------

    /** Thrown when the Avro data file backing a table is absent. */
    static final String MSG_FILE_NOT_FOUND = "Avro data file does not exist: ";

    /** Thrown when the 16-byte block sync marker does not match the file content. */
    static final String MSG_SYNC_MARKER_MISMATCH =
            "Avro block sync marker mismatch at offset ";

    /** Thrown when a read hits end-of-file before the declared payload length. */
    static final String MSG_TRUNCATED_FILE = "Truncated Avro data file ";

    /** Thrown when a read hits end-of-file while decoding a block header varint. */
    static final String MSG_TRUNCATED_BLOCK = "Truncated Avro block at ";

    /** Suffix shared by the truncated-file messages, appended after the offset. */
    static final String MSG_BYTES_AT_OFFSET = " bytes at offset ";

    /** Suffix reporting where the file physically ended, appended after a length. */
    static final String MSG_FILE_ENDS_AT = ", file ends at ";

    /** Suffix reporting the expected value, appended after a header field. */
    static final String MSG_EXPECTED = ": expected ";

    /** Thrown when a sync marker is absent where the block header promised one. */
    static final String MSG_EXPECTED_SYNC_MARKER = ": expected sync marker at ";

    /** Thrown when a block header count or size field is malformed. */
    static final String MSG_INVALID_BLOCK_HEADER =
            "Invalid Avro block header (count=";

    /** Suffix separating the count field from the size field of a block header. */
    static final String MSG_SIZE_FIELD = ", size=";

    /** Thrown when a codec name is not one of the supported Avro codecs. */
    static final String MSG_UNSUPPORTED_CODEC = "Unsupported Avro codec '";

    // ---- System property keys ----------------------------------------------------

    /** Overrides the config file used to resolve schema-evolution settings. */
    static final String PROP_SCHEMA_CONFIG_FILE = "avro.schema.config.file";

    // ---- Backup manifest ---------------------------------------------------------

    /** Name of the manifest describing a backup snapshot. */
    static final String MANIFEST_FILE = "manifest.txt";

    /**
     * Keys of the {@code key=value} lines that make up a backup manifest. Each
     * key is read back three times when a manifest is parsed (prefix test,
     * length, substring), so they are named rather than repeated inline.
     */
    static final String MANIFEST_KEY_BACKUP_TYPE = "backup_type=";
    static final String MANIFEST_KEY_SOURCE_DIR = "source_dir=";
    static final String MANIFEST_KEY_BACKUP_DIR = "backup_dir=";
    static final String MANIFEST_KEY_STARTED_AT = "started_at=";
    static final String MANIFEST_KEY_COMPLETED_AT = "completed_at=";
    static final String MANIFEST_KEY_TOTAL_FILES = "total_files=";
    static final String MANIFEST_KEY_TOTAL_BYTES = "total_bytes=";
    static final String MANIFEST_KEY_DURATION_NANOS = "duration_nanos=";
    static final String MANIFEST_KEY_FILES_FAILED = "files_failed=";
    static final String MANIFEST_KEY_FILE = "file=";

    /** Field separator inside a manifest {@code file=} record. */
    static final String MANIFEST_FIELD_SEPARATOR = "|";

    private AvroFileConstants() {
    }
}

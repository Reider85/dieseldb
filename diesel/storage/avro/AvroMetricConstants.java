package diesel.storage.avro;

/**
 * Shared literals for the AVRO storage module.
 *
 * <p>Centralises the string literals that were previously duplicated across
 * metrics, audit and config-loading classes so that they are declared exactly
 * once (SonarQube S1192).
 *
 * @since Prompt 11
 */
final class AvroMetricConstants {

    /** Prefix used for every metric-related log line. */
    static final String LOG_FORMAT_AVRO_METRIC = "[AVRO-METRIC] {}";

    /** Prometheus metric type for monotonically increasing values. */
    static final String METRIC_TYPE_COUNTER = "counter";

    /** Prometheus metric type for values that can go up and down. */
    static final String METRIC_TYPE_GAUGE = "gauge";

    /** JMX {@code MBeanAttributeInfo} type name for {@code long} attributes. */
    static final String MXBEAN_TYPE_LONG = "long";

    /** JMX {@code MBeanAttributeInfo} type name for {@code double} attributes. */
    static final String MXBEAN_TYPE_DOUBLE = "double";

    /** Logged when a quoted raw config value fails to parse. */
    static final String MSG_INVALID_VALUE_QUOTED =
            "Invalid {} value '{}', using default {}";

    /** Logged when a numeric config value fails validation. */
    static final String MSG_INVALID_VALUE_UNQUOTED =
            "Invalid {} value {}, using default {}";

    /** Logged when a config value fails to parse, quoting the raw text. */
    static final String MSG_INVALID_VALUE_QUOTED_EQUALS =
            "Invalid {} = \"{}\", using default {}";

    /** Logged when {@code config.properties} cannot be read. */
    static final String MSG_CONFIG_READ_FAILED =
            "Could not read config.properties, using defaults: {}";

    private AvroMetricConstants() {
    }
}

package diesel;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Gates a test class so it only runs when the active
 * {@code diesel.storage.type} matches one of the declared values.
 *
 * <p>Usage:
 * <pre>{@code
 * @StorageType("avro")
 * class AvroCompressionTest { ... }
 *
 * @StorageType({"jsonl", "csv"})
 * class JsonlLoadModeTest { ... }
 * }</pre>
 *
 * <p>When {@code diesel.storage.type=csv}, classes annotated with
 * {@code @StorageType("avro")} are skipped entirely.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(StorageTypeCondition.class)
public @interface StorageType {
    /**
     * One or more storage type names that enable this test class.
     * Valid values: {@code csv}, {@code tsv}, {@code jsonl}, {@code avro}, {@code in_memory}.
     */
    String[] value();
}

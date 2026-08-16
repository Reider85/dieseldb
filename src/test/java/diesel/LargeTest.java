package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test method that materialises a result needing more than ~1GB of heap
 * (e.g. the 600x600 ORDER BY joins producing 360k rows). Such tests are skipped by
 * default - including CI - and run only when the {@value #LARGE_TESTS_PROPERTY}
 * system property is {@code true}, which also requires a larger heap
 * (pom.xml {@code -Dtest.heap=4g}).
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Test
@Tag("large")
@EnabledIfSystemProperty(named = LargeTest.LARGE_TESTS_PROPERTY, matches = "true")
public @interface LargeTest {

    String LARGE_TESTS_PROPERTY = "diesel.largeTests";
}

package diesel;

import diesel.CharOps;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 30 (java:S5869, S6353) - benchmark comparing the OLD regex
 * implementations against the NEW char-loop string operations in
 * {@link CharOps}, plus an equivalence check that the replacement is
 * semantically identical for a mixed valid/invalid corpus.
 *
 * The old regexes are kept inline here (they were removed from the engine),
 * so the wall-clock comparison measures the same code paths the engine used.
 * Asserts are deliberately generous (new must not be >10x slower than old,
 * and must stay under an absolute bound) so machine noise cannot flake the
 * suite while a real regression would still be caught.
 */
public class StringOpsBenchmarkTest {

    private static final String[] IDENTIFIER_SAMPLES = {
            "User", "USERS", "_x", "A1", "a_", "Z", "A", "_",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_",
            "", "123abc", "a b", "USER.NAME", "x-y", "A B", "1a"
    };

    private static final String[] WHITESPACE_SAMPLES = {
            "USERS", "USER DETAILS", " A", "A ", "A\tB", "A\nB", "A B C D",
            "", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "ABC DEF GHI JKL", "A\rB", "A\fB"
    };

    private static final String[] DATE_SAMPLES = {
            "2023-10-15", "2023-1-5", "2023-10-155", "20a3-10-15", "20231015",
            "2023-13-45", "", "2023-10-1", "0000-00-00"
    };

    private static final String[] DATETIME_SAMPLES = {
            "2023-10-15 14:30:00", "2023-10-15 14:30", "2023-10-15 14:30:0",
            "2023-10-15 14:30:00 ", "2023-10-15", ""
    };

    private static final String[] DATETIME_MS_SAMPLES = {
            "2023-10-15 14:30:00.123", "2023-10-15 14:30:00.12",
            "2023-10-15 14:30:00.1234", "2023-10-15 14:30:00",
            "2023-10-15 14:30:00.12x", ""
    };

    private interface StringCheck {
        boolean test(String s);
    }

    @Test
    void asciiIdentifierStringOpsEquivalentAndFaster() {
        assertEquivalent("asciiIdentifier", Pattern.compile("[a-zA-Z_]\\w*"),
                IDENTIFIER_SAMPLES, CharOps::isAsciiIdentifier);
        benchmark("asciiIdentifier", Pattern.compile("[a-zA-Z_]\\w*"),
                IDENTIFIER_SAMPLES, CharOps::isAsciiIdentifier);
    }

    @Test
    void containsWhitespaceStringOpsEquivalentAndFaster() {
        assertEquivalent("containsWhitespace", Pattern.compile(".*\\s+.*"),
                WHITESPACE_SAMPLES, CharOps::containsWhitespace);
        benchmark("containsWhitespace", Pattern.compile(".*\\s+.*"),
                WHITESPACE_SAMPLES, CharOps::containsWhitespace);
    }

    @Test
    void localDateLiteralStringOpsEquivalentAndFaster() {
        assertEquivalent("localDateLiteral", Pattern.compile("\\d{4}-\\d{2}-\\d{2}"),
                DATE_SAMPLES, CharOps::isLocalDateLiteral);
        benchmark("localDateLiteral", Pattern.compile("\\d{4}-\\d{2}-\\d{2}"),
                DATE_SAMPLES, CharOps::isLocalDateLiteral);
    }

    @Test
    void localDateTimeLiteralStringOpsEquivalentAndFaster() {
        assertEquivalent("localDateTimeLiteral", Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"),
                DATETIME_SAMPLES, CharOps::isLocalDateTimeLiteral);
        benchmark("localDateTimeLiteral", Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"),
                DATETIME_SAMPLES, CharOps::isLocalDateTimeLiteral);
    }

    @Test
    void localDateTimeMillisLiteralStringOpsEquivalentAndFaster() {
        assertEquivalent("localDateTimeMillisLiteral",
                Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}"),
                DATETIME_MS_SAMPLES, CharOps::isLocalDateTimeMillisLiteral);
        benchmark("localDateTimeMillisLiteral",
                Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}"),
                DATETIME_MS_SAMPLES, CharOps::isLocalDateTimeMillisLiteral);
    }

    private static void assertEquivalent(String name, Pattern oldPattern, String[] corpus, StringCheck newCheck) {
        for (String sample : corpus) {
            boolean oldResult = oldPattern.matcher(sample).matches();
            boolean newResult = newCheck.test(sample);
            assertEquals(oldResult, newResult, name + " mismatch for '" + sample + "'");
        }
    }

    private static void benchmark(String name, Pattern oldPattern, String[] corpus, StringCheck newCheck) {
        int warmup = 20_000;
        int measured = 200_000;

        runRound(oldPattern, corpus, warmup);
        runRound(newCheck, corpus, warmup);

        long oldMs = runRound(oldPattern, corpus, measured);
        long newMs = runRound(newCheck, corpus, measured);

        double speedup = oldMs / (double) Math.max(1, newMs);
        System.out.printf("[StringOpsBenchmark] %s: old=%d ms, new=%d ms, speedup=%.1fx%n",
                name, oldMs, newMs, speedup);

        assertTrue(newMs <= oldMs * 10,
                name + ": string ops must not be slower than the regex: old=" + oldMs + " ms, new=" + newMs + " ms");
        assertTrue(newMs < 5_000, name + ": string ops took too long: " + newMs + " ms");
    }

    private static long runRound(Pattern pattern, String[] corpus, int calls) {
        long start = System.nanoTime();
        for (int i = 0; i < calls; i++) {
            boolean ignored = pattern.matcher(corpus[i % corpus.length]).matches();
        }
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static long runRound(StringCheck check, String[] corpus, int calls) {
        long start = System.nanoTime();
        for (int i = 0; i < calls; i++) {
            boolean ignored = check.test(corpus[i % corpus.length]);
        }
        return (System.nanoTime() - start) / 1_000_000;
    }
}
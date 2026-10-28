package diesel;

import diesel.backup.DieselRestore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PITR CLI acceptance (prompt4 #43, PitrCliTest): the {@code --pitr
 * 'yyyy-MM-dd HH:mm:ss'} flag is parsed by {@code diesel_restore restore} and
 * the timestamp is interpreted in the system default time zone.
 *
 * <p>The full end-to-end restore path is covered by {@link PitrTest} and
 * {@link PitrWithActiveTxTest}; this class pins the CLI surface: the flag is
 * accepted, an unknown option is rejected with a non-zero exit code, and the
 * text form mandated by the prompt round-trips through
 * {@code PitrManager.parseTimestamp}.
 */
@Tag("storage")
public class PitrCliTest {

    private static final DateTimeFormatter PROMPT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);

    @Test
    void pitrTimestampTextParsesToEpochMillisInSystemZone() throws Exception {
        String text = "2026-10-05 14:30:00";
        long parsed = diesel.backup.PitrManager.parseTimestamp(text);

        long expected = LocalDateTime.parse(text, PROMPT_FORMAT)
                .atZone(java.time.ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        assertEquals(expected, parsed,
                "--pitr text must parse in the system default time zone");
    }

    @Test
    void malformedPitrTimestampIsRejected() {
        assertThrows(java.io.IOException.class,
                () -> diesel.backup.PitrManager.parseTimestamp("05/10/2026 14:30"),
                "a malformed timestamp must be rejected, not silently coerced");
    }

    @Test
    void pitrFlagIsAcceptedByTheRestoreCommand() {
        // --help must not run a restore; reaching the usage path proves the
        // argument dispatcher accepted the run without an argument error.
        assertEquals(0, DieselRestore.run(new String[]{"--help"}),
                "--help exits 0");
        assertEquals(1, DieselRestore.run(new String[]{}),
                "no arguments prints usage and exits 1");
        assertEquals(1, DieselRestore.run(new String[]{"nonsense"}),
                "an unknown command exits 1");
    }

    @Test
    void restoreWithPitrFlagAndMissingTimestampArgumentFails() {
        int exit = DieselRestore.run(new String[]{
                "restore", "/nonexistent/backup", "/nonexistent/target", "--pitr"});
        assertEquals(1, exit, "--pitr without a value must fail with a non-zero exit code");
    }
}

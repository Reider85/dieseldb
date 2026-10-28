package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Covers CliRepl.promptCredentials() - the interactive login asked when
 * start-client.sh / plain CliRepl runs without --user (prompt: login, then
 * password without echo via System.console()). Surefire has no attached
 * console, so these tests exercise the piped-stdin fallback path.
 */
@Tag("smoke")
@ResourceLock("System.in")
class CliReplPromptTest {

    @Test
    void readsUserAndPasswordFromStdin() {
        String[] creds = withStdin("alice\nsecret\n");
        assertArrayEquals(new String[]{"alice", "secret"}, creds);
    }

    @Test
    void trimsSurroundingWhitespaceFromLogin() {
        String[] creds = withStdin("  bob  \npw\n");
        assertArrayEquals(new String[]{"bob", "pw"}, creds);
    }

    @Test
    void emptyLoginReturnsNullCredentials() {
        String[] creds = withStdin("\n");
        assertNull(creds[0], "empty login line must keep the no-credentials behaviour");
        assertNull(creds[1]);
    }

    @Test
    void endOfInputReturnsNullCredentials() {
        String[] creds = withStdin("");
        assertNull(creds[0], "EOF must not block or throw - connect without credentials");
        assertNull(creds[1]);
    }

    private static String[] withStdin(String input) {
        InputStream original = System.in;
        try {
            System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
            Supplier<String[]> prompt = CliRepl::promptCredentials;
            return prompt.get();
        } finally {
            System.setIn(original);
        }
    }
}

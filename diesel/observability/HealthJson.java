package diesel.observability;

import java.util.List;

/**
 * Minimal JSON assembly for the health endpoints (prompt4 #46, R3-012 step 3/3).
 *
 * <p>The health payloads are flat, fixed-shape documents, so pulling in a JSON
 * library (or reusing the row-storage parser) would be pure overhead. This
 * helper only covers the two things those documents need: quoting/escaping a
 * string and rendering an array of strings.
 */
final class HealthJson {

    private HealthJson() {
    }

    /**
     * Escapes a string for inclusion inside JSON double quotes.
     *
     * @param value the raw value, may be null
     * @return the escaped value ({@code null} becomes the literal {@code null})
     */
    static String escape(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * Renders a value as a JSON string literal.
     *
     * @param value the raw value
     * @return the quoted, escaped literal
     */
    static String quote(String value) {
        return "\"" + escape(value) + "\"";
    }

    /**
     * Renders a list of strings as a JSON array (empty list becomes {@code []}).
     *
     * @param values the values to render
     * @return the JSON array literal
     */
    static String array(List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(quote(values.get(i)));
        }
        return sb.append(']').toString();
    }
}

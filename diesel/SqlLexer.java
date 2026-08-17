package diesel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tokenizer that splits a SQL statement into a sequence of {@link Token}s.
 *
 * <p>It recognizes keywords, identifiers (including double-quoted ones),
 * integer and decimal numbers, single-quoted string literals (with
 * doubled-quote and backslash escapes), the SQL literals TRUE/FALSE/NULL,
 * comparison operators and punctuation. Unterminated literals and unexpected
 * characters raise an {@link IllegalArgumentException}.
 *
 * @see QueryParser
 */
public class SqlLexer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlLexer.class);

    /**
     * The lexical category of a {@link Token}.
     */
    public enum TokenType {
        KEYWORD,
        IDENTIFIER,
        QUOTED_IDENTIFIER,
        INTEGER,
        DECIMAL,
        STRING_LITERAL,
        LITERAL,
        COMPARISON_OPERATOR,
        PUNCTUATION
    }

    /**
     * A single lexer token: its {@link TokenType} and raw text value.
     */
    public static class Token {
        public final TokenType type;
        public final String value;

        Token(TokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Token{type=" + type + ", value='" + value + "'}";
        }
    }

    private static final Set<String> KEYWORDS = new HashSet<>(Set.of(
            SqlKeywords.SELECT, "FROM", SqlKeywords.WHERE, SqlKeywords.INSERT, "INTO", SqlKeywords.VALUES, SqlKeywords.UPDATE, SqlKeywords.SET, SqlKeywords.DELETE,
            "CREATE", SqlKeywords.TABLE, "INDEX", "HASH", "UNIQUE", "CLUSTERED", "PRIMARY", "KEY", "SEQUENCE",
            SqlKeywords.AND, SqlKeywords.OR, SqlKeywords.NOT, SqlKeywords.LIKE, "IN", "IS", SqlKeywords.AS, SqlKeywords.JOIN,
            "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "CROSS", SqlKeywords.ON, "GROUP", "BY", "ORDER",
            SqlKeywords.HAVING, SqlKeywords.LIMIT, SqlKeywords.ASC, "DESC", "DISTINCT", "BEGIN", "TRANSACTION", "COMMIT",
            "ROLLBACK", "ISOLATION", "LEVEL", "AUTOCOMMIT"));

    private static final Set<String> LITERALS = new HashSet<>(Set.of(SqlKeywords.TRUE, SqlKeywords.FALSE, SqlKeywords.NULL));

    private static final String[] OPERATORS = {">=", "<=", "!=", "<>", "=", "<", ">"};
    private static final String PUNCTUATION_CHARS = "(),;.*+-/%[]?:" + "'";

    /**
     * Extracts the content of a single-quoted string literal, removing the
     * surrounding quotes and resolving SQL escapes (doubled quotes {@code ''}
     * and backslash escapes such as {@code \'}). For example the token
     * {@code 'John'} yields {@code John} and {@code 'it''s'} yields
     * {@code it's}. Input that is not a single-quoted literal is returned
     * unchanged.
     */
    public static String extractStringLiteral(String token) {
        if (token == null || token.length() < 2
                || token.charAt(0) != '\'' || token.charAt(token.length() - 1) != '\'') {
            return token;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < token.length() - 1; i++) {
            char c = token.charAt(i);
            if (c == '\'' && i + 1 < token.length() - 1 && token.charAt(i + 1) == '\'') {
                sb.append('\'');
                i++;
            } else if (c == '\\' && i + 1 < token.length() - 1) {
                sb.append(token.charAt(i + 1));
                i++;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Tokenizes a SQL statement.
     *
     * @param sql the SQL statement to tokenize
     * @return the ordered list of tokens
     * @throws IllegalArgumentException if the input is null, contains an
     *                                  unterminated literal, or an unexpected character
     */
    public List<Token> tokenize(String sql) {
        if (sql == null) {
            throw new IllegalArgumentException("SQL query cannot be null");
        }
        List<Token> tokens = new ArrayList<>();
        int pos = 0;
        int length = sql.length();

        while (pos < length) {
            char c = sql.charAt(pos);

            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }

            if (c == '\'') {
                StringBuilder sb = new StringBuilder();
                sb.append(c);
                pos++;
                boolean closed = false;
                while (pos < length) {
                    char ch = sql.charAt(pos);
                    sb.append(ch);
                    pos++;
                    if (ch == '\'') {
                        if (pos < length && sql.charAt(pos) == '\'') {
                            sb.append(sql.charAt(pos));
                            pos++;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    if (ch == '\\' && pos < length) {
                        sb.append(sql.charAt(pos));
                        pos++;
                    }
                }
                if (!closed) {
                    throw new IllegalArgumentException("Unterminated string literal at position " + pos);
                }
                tokens.add(new Token(TokenType.STRING_LITERAL, sb.toString()));
                continue;
            }

            if (c == '"') {
                StringBuilder sb = new StringBuilder();
                pos++;
                boolean closed = false;
                while (pos < length) {
                    char ch = sql.charAt(pos);
                    if (ch == '"') {
                        pos++;
                        closed = true;
                        break;
                    }
                    sb.append(ch);
                    pos++;
                }
                if (!closed) {
                    throw new IllegalArgumentException("Unterminated quoted identifier at position " + pos);
                }
                tokens.add(new Token(TokenType.QUOTED_IDENTIFIER, sb.toString()));
                continue;
            }

            if (Character.isDigit(c)) {
                StringBuilder sb = new StringBuilder();
                boolean isDecimal = false;
                while (pos < length && (Character.isDigit(sql.charAt(pos)) || sql.charAt(pos) == '.')) {
                    if (sql.charAt(pos) == '.') {
                        if (isDecimal) {
                            break;
                        }
                        isDecimal = true;
                    }
                    sb.append(sql.charAt(pos));
                    pos++;
                }
                tokens.add(new Token(isDecimal ? TokenType.DECIMAL : TokenType.INTEGER, sb.toString()));
                continue;
            }

            if (Character.isLetter(c) || c == '_') {
                StringBuilder sb = new StringBuilder();
                while (pos < length && (Character.isLetterOrDigit(sql.charAt(pos)) || sql.charAt(pos) == '_')) {
                    sb.append(sql.charAt(pos));
                    pos++;
                }
                String word = sb.toString();
                String upper = word.toUpperCase();
                if (LITERALS.contains(upper)) {
                    tokens.add(new Token(TokenType.LITERAL, upper));
                } else if (KEYWORDS.contains(upper)) {
                    tokens.add(new Token(TokenType.KEYWORD, upper));
                } else {
                    tokens.add(new Token(TokenType.IDENTIFIER, word));
                }
                continue;
            }

            boolean operatorMatched = false;
            for (String op : OPERATORS) {
                if (sql.startsWith(op, pos)) {
                    tokens.add(new Token(TokenType.COMPARISON_OPERATOR, op));
                    pos += op.length();
                    operatorMatched = true;
                    break;
                }
            }
            if (operatorMatched) {
                continue;
            }

            if (PUNCTUATION_CHARS.indexOf(c) >= 0) {
                tokens.add(new Token(TokenType.PUNCTUATION, String.valueOf(c)));
                pos++;
                continue;
            }

            throw new IllegalArgumentException("Unexpected character '" + c + "' at position " + pos);
        }

        return tokens;
    }

    /**
     * Demo entry point that tokenizes a handful of example queries and logs
     * the resulting tokens.
     *
     * @param args not used
     */
    public static void main(String[] args) {
        SqlLexer lexer = new SqlLexer();
        String[] queries = {
                "SELECT * FROM users WHERE age >= 30 AND name = 'John'",
                "SELECT \"Id\", NAME FROM users",
                "INSERT INTO t (id, name) VALUES (1, 'Alice')",
                "UPDATE users SET age = 31 WHERE id = 5",
                "DELETE FROM users WHERE balance <> 0 OR flag = TRUE",
                "SELECT COUNT(*) FROM t WHERE name IS NULL"
        };
        for (String query : queries) {
            LOGGER.info("Query: {}", query);
            try {
                for (Token token : lexer.tokenize(query)) {
                    LOGGER.info("  {}", token);
                }
            } catch (IllegalArgumentException e) {
                LOGGER.error("  ERROR: {}", e.getMessage());
            }
            LOGGER.info("");
        }
    }
}

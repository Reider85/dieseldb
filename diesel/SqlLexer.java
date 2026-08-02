package diesel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlLexer {

    public enum TokenType {
        KEYWORD,
        IDENTIFIER,
        QUOTED_IDENTIFIER,
        INTEGER,
        DECIMAL,
        STRING_LITERAL,
        COMPARISON_OPERATOR,
        PUNCTUATION
    }

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
            "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
            "CREATE", "TABLE", "INDEX", "HASH", "UNIQUE", "CLUSTERED", "PRIMARY", "KEY", "SEQUENCE",
            "AND", "OR", "NOT", "LIKE", "IN", "IS", "NULL", "TRUE", "FALSE", "AS", "JOIN",
            "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "CROSS", "ON", "GROUP", "BY", "ORDER",
            "HAVING", "LIMIT", "ASC", "DESC", "DISTINCT", "BEGIN", "TRANSACTION", "COMMIT",
            "ROLLBACK", "ISOLATION", "LEVEL", "AUTOCOMMIT", "SAVEPOINT"));

    private static final String[] OPERATORS = {">=", "<=", "!=", "<>", "=", "<", ">"};
    private static final String PUNCTUATION_CHARS = "(),;.*+-/%[]?:" + "'";

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
                if (KEYWORDS.contains(upper)) {
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
            System.out.println("Query: " + query);
            try {
                for (Token token : lexer.tokenize(query)) {
                    System.out.println("  " + token);
                }
            } catch (IllegalArgumentException e) {
                System.out.println("  ERROR: " + e.getMessage());
            }
            System.out.println();
        }
    }
}

package diesel;

import java.util.logging.Level;
import java.util.logging.Logger;

public class NormalizationUtils {
    private static final Logger LOGGER = Logger.getLogger(NormalizationUtils.class.getName());

    public static String normalizeQuery(String query) {
        StringBuilder normalized = new StringBuilder();
        boolean inQuotes = false;
        boolean inFunction = false;
        int parenDepth = 0;
        StringBuilder currentToken = new StringBuilder();
        boolean isAggregateFunction = false;

        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentToken.append(c);
                continue;
            }
            if (inQuotes) {
                currentToken.append(c);
                continue;
            }
            if (c == '(') {
                parenDepth++;
                String tokenSoFar = currentToken.toString().trim().toUpperCase();
                if (parenDepth == 1 && (tokenSoFar.equals("COUNT") || tokenSoFar.equals("MIN") ||
                        tokenSoFar.equals("MAX") || tokenSoFar.equals("AVG") || tokenSoFar.equals("SUM"))) {
                    inFunction = true;
                    isAggregateFunction = true;
                }
                currentToken.append(c);
                continue;
            }
            if (c == ')') {
                parenDepth--;
                currentToken.append(c);
                if (parenDepth == 0 && inFunction) {
                    inFunction = false;
                    isAggregateFunction = false;
                    String functionToken = currentToken.toString().trim();
                    LOGGER.log(Level.FINE, "Preserving aggregate function: {0}", functionToken);
                    normalized.append(functionToken);
                    currentToken = new StringBuilder();
                    continue;
                }
                continue;
            }
            if (Character.isWhitespace(c) || c == ',' || c == ';') {
                if (currentToken.length() > 0) {
                    String token = currentToken.toString().trim();
                    if (inFunction) {
                        normalized.append(token);
                    } else if (!token.isEmpty()) {
                        normalized.append(token.toUpperCase());
                    }
                    currentToken = new StringBuilder();
                }
                normalized.append(c);
                continue;
            }
            currentToken.append(c);
        }

        if (currentToken.length() > 0) {
            String token = currentToken.toString().trim();
            if (inFunction) {
                normalized.append(token);
            } else if (!token.isEmpty()) {
                normalized.append(token.toUpperCase());
            }
        }

        String result = normalized.toString().trim();
        LOGGER.log(Level.FINE, "Normalized query result: {0}", result);
        return result;
    }

    public static String normalizeColumnName(String column, String tableName) {
        if (column == null || column.isEmpty()) {
            return column;
        }
        String normalized = column.trim();
        if (tableName != null && !normalized.contains(".")) {
            normalized = tableName + "." + normalized;
        }
        return normalized;
    }

    public static String convertLikePatternToRegex(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("LIKE pattern cannot be null or empty");
        }

        StringBuilder escapedPattern = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (".^$*+?()[{\\|".indexOf(c) >= 0) {
                escapedPattern.append('\\').append(c);
            } else {
                escapedPattern.append(c);
            }
        }

        String regex = escapedPattern.toString()
                .replace("%", ".*")
                .replace("_", ".");

        return "^" + regex + "$";
    }
}
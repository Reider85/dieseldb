package diesel;

/**
 * Prompt 12 (java:S1192): String literals that repeat 3+ times inside
 * {@link SubqueryParser}, extracted into named constants to prevent spelling
 * drift and reduce code duplication. Behaviour is identical to using the inline
 * literal - this class only centralises the text.
 *
 * <p>Literals that appear fewer than three times, or that belong to a shared
 * vocabulary ({@link SqlKeywords}, {@link ErrorMessages},
 * {@link MessageConstants}), are deliberately left inline.
 */
public final class SubqueryConstants {

    private SubqueryConstants() {
    }

    // ---- Generic text fragments -------------------------------------------------

    /** Regex matching a run of one or more whitespace characters. */
    public static final String WHITESPACE_PATTERN = "\\s+";

    /** Regex matching a literal dot, used to split a qualified {@code table.column} name. */
    public static final String DOT_SEPARATOR = "\\.";

    /** Separator between an error-message prefix and its variable part. */
    public static final String COLON_SPACE = ": ";

    /** Closing of an optional group, e.g. an optional {@code AS alias} suffix. */
    public static final String OPTIONAL_GROUP_CLOSE = ")?";

    /** Replacement collapsing any run of whitespace down to a single space. */
    public static final String SINGLE_SPACE = " ";

    // ---- Quoted-literal and nested-expression regex bodies -----------------------

    /**
     * Regex matching a single-quoted SQL string literal, including backslash
     * escapes. Repeated in every tokenizer and subquery-recognition pattern.
     */
    public static final String QUOTED_STRING_PATTERN = "'(?:\\\\.|[^'\\\\])*+'";

    /**
     * Opening of the alternation used to consume a balanced subquery body: any
     * run of characters that are neither parens nor quotes, a quoted string, or
     * a nested parenthesised group. Must be closed with {@code )*+}.
     */
    public static final String NESTED_EXPR_BODY = "(?:[^()']++|" + QUOTED_STRING_PATTERN + "|\\([^()]*+\\)";

    /** Possessive close of {@link #NESTED_EXPR_BODY}. */
    public static final String NESTED_EXPR_CLOSE = ")*+";

    /** Tokenizer pattern matching any run of non-whitespace characters. */
    public static final String NON_WHITESPACE_RUN = "[^\\s()']++";

    // ---- Clause shapes ---------------------------------------------------------

    /** Regex for a trailing {@code LIMIT n [OFFSET m]} clause, end-anchored. */
    public static final String LIMIT_OFFSET_TAIL_PATTERN =
            "(?i)\\s*LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?\\s*$";

    /** Regex for a leading {@code WHERE} keyword and the whitespace after it. */
    public static final String LEADING_WHERE_PATTERN = "(?i)^\\s*WHERE\\s+";

    /** Regex for a word boundary, used when composing keyword patterns. */
    public static final String WORD_BOUNDARY = "\\b";

    /** Prefix of every case-insensitive keyword pattern. */
    public static final String CASE_INSENSITIVE = "(?i)";

    // ---- Tokenizer pattern names ------------------------------------------------

    public static final String TOKEN_QUOTED_STRING = "Quoted String";
    public static final String TOKEN_GROUPED_CONDITION = "Grouped Condition";
    public static final String TOKEN_IN_CONDITION = "In Condition";
    public static final String TOKEN_SUBQUERY_COMPARISON = "Subquery Comparison";
    public static final String TOKEN_SUBQUERY_LIKE = "Subquery Like";
    public static final String TOKEN_NULL_CONDITION = "Null Condition";
    public static final String TOKEN_COMPARISON_CONDITION = "Comparison Condition";
    public static final String TOKEN_TABLE_ALIAS = "Table Alias";
    public static final String TOKEN_NOT_KEYWORD = "NOT Keyword";
    public static final String TOKEN_ALIAS = "Alias";
}

package diesel.format;

import java.util.List;

/**
 * Format-agnostic predicate used for predicate pushdown into a
 * {@link TableFormat} reader. The engine converts its internal WHERE
 * conditions into this shape; a format that advertises
 * {@link FormatCapabilities#supportsPredicatePushdown()} may translate these
 * into file-level filters or row-group statistics pruning. Formats without
 * pushdown simply ignore them and return all rows.
 *
 * <p>Values carry the same semantics as the engine's equality/comparison
 * operators on {@link String}, {@link Number}, {@link java.time.LocalDate},
 * {@link java.time.LocalDateTime} and {@link Boolean} values.</p>
 */
public final class ColumnPredicate {

    /** Comparison operators expressible for pushdown. */
    public enum Operator {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        GREATER_THAN,
        LESS_THAN_OR_EQUALS,
        GREATER_THAN_OR_EQUALS,
        IN,
        IS_NULL,
        IS_NOT_NULL
    }

    private final String column;
    private final Operator operator;
    private final Object value;
    private final List<Object> values;
    private final boolean negated;

    private ColumnPredicate(String column, Operator operator, Object value,
                            List<Object> values, boolean negated) {
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.values = values;
        this.negated = negated;
    }

    /**
     * Creates a scalar predicate (EQUALS, comparisons).
     *
     * @param column   the column name
     * @param operator the operator
     * @param value    the comparison value
     * @param negated  whether the condition is negated (NOT)
     * @return the predicate
     */
    public static ColumnPredicate of(String column, Operator operator, Object value, boolean negated) {
        return new ColumnPredicate(column, operator, value, List.of(), negated);
    }

    /**
     * Creates an IN-list predicate.
     *
     * @param column   the column name
     * @param values   the candidate values
     * @param negated  whether the condition is negated (NOT IN)
     * @return the predicate
     */
    public static ColumnPredicate in(String column, List<Object> values, boolean negated) {
        return new ColumnPredicate(column, Operator.IN, null,
                values == null ? List.of() : List.copyOf(values), negated);
    }

    /**
     * Creates an IS NULL / IS NOT NULL predicate.
     *
     * @param column  the column name
     * @param isNull  whether the predicate tests for NULL
     * @return the predicate
     */
    public static ColumnPredicate nullTest(String column, boolean isNull) {
        return new ColumnPredicate(column, isNull ? Operator.IS_NULL : Operator.IS_NOT_NULL,
                null, List.of(), false);
    }

    public String getColumn() {
        return column;
    }

    public Operator getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }

    public List<Object> getValues() {
        return values;
    }

    public boolean isNegated() {
        return negated;
    }

    public boolean isIn() {
        return operator == Operator.IN;
    }

    public boolean isNullOperator() {
        return operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL;
    }

    @Override
    public String toString() {
        return "ColumnPredicate{" + column + ' ' + operator + ' ' + value + '}';
    }
}
package diesel;

import java.util.List;
import java.util.stream.Collectors;

class Condition {
    String column;
    Object value;
    String rightColumn;
    List<Object> inValues;
    QueryParser.Operator operator;
    String conjunction;
    boolean not;
    List<Condition> subConditions;

    Condition(String column, Object value, QueryParser.Operator operator, String conjunction, boolean not) {
        this.column = column;
        this.value = value;
        this.rightColumn = null;
        this.inValues = null;
        this.operator = operator;
        this.conjunction = conjunction;
        this.not = not;
        this.subConditions = null;
    }

    Condition(String column, List<Object> inValues, String conjunction, boolean not) {
        this.column = column;
        this.value = null;
        this.rightColumn = null;
        this.inValues = inValues;
        this.operator = QueryParser.Operator.IN;
        this.conjunction = conjunction;
        this.not = not;
        this.subConditions = null;
    }

    Condition(List<Condition> subConditions, String conjunction, boolean not) {
        this.column = null;
        this.value = null;
        this.rightColumn = null;
        this.inValues = null;
        this.operator = null;
        this.conjunction = conjunction;
        this.not = not;
        this.subConditions = subConditions;
    }

    Condition(String column, String rightColumn, QueryParser.Operator operator, String conjunction, boolean not) {
        this.column = column;
        this.value = null;
        this.rightColumn = rightColumn;
        this.inValues = null;
        this.operator = operator;
        this.conjunction = conjunction;
        this.not = not;
        this.subConditions = null;
    }

    Condition(String column, QueryParser.Operator operator, String conjunction, boolean not) {
        this.column = column;
        this.value = null;
        this.rightColumn = null;
        this.inValues = null;
        this.operator = operator;
        this.conjunction = conjunction;
        this.not = not;
        this.subConditions = null;
    }

    boolean isGrouped() {
        return subConditions != null;
    }

    boolean isInOperator() {
        return operator == QueryParser.Operator.IN;
    }

    boolean isColumnComparison() {
        return rightColumn != null;
    }

    boolean isNullOperator() {
        return operator == QueryParser.Operator.IS_NULL || operator == QueryParser.Operator.IS_NOT_NULL;
    }

    @Override
    public String toString() {
        if (isGrouped()) {
            String subCondStr = subConditions.stream()
                    .map(Condition::toString)
                    .collect(Collectors.joining(" "));
            return (not ? "NOT " : "") + "(" + subCondStr + ")" + (conjunction != null ? " " + conjunction : "");
        }
        if (isInOperator()) {
            String valuesStr = inValues.stream()
                    .map(v -> v instanceof String ? "'" + v + "'" : v.toString())
                    .collect(Collectors.joining(", "));
            return (not ? "NOT " : "") + column + " IN (" + valuesStr + ")" + (conjunction != null ? " " + conjunction : "");
        }
        if (isColumnComparison()) {
            String operatorStr = switch (operator) {
                case LIKE -> "LIKE";
                case NOT_LIKE -> "NOT LIKE";
                default -> operator.toString();
            };
            return (not ? "NOT " : "") + column + " " + operatorStr + " " + rightColumn + (conjunction != null ? " " + conjunction : "");
        }
        if (isNullOperator()) {
            String operatorStr = operator == QueryParser.Operator.IS_NULL ? "IS NULL" : "IS NOT NULL";
            return (not ? "NOT " : "") + column + " " + operatorStr + (conjunction != null ? " " + conjunction : "");
        }
        String operatorStr = switch (operator) {
            case LIKE -> "LIKE";
            case NOT_LIKE -> "NOT LIKE";
            default -> operator.toString();
        };
        return (not ? "NOT " : "") + column + " " + operatorStr + " " + (value instanceof String ? "'" + value + "'" : value) + (conjunction != null ? " " + conjunction : "");
    }
}
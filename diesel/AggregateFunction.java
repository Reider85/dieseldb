package diesel;

class AggregateFunction {
    String functionName;
    String column;
    String alias;

    AggregateFunction(String functionName, String column, String alias) {
        this.functionName = functionName.toUpperCase();
        this.column = column;
        this.alias = alias;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(functionName).append("(");
        sb.append(column == null ? "*" : column);
        sb.append(")");
        if (alias != null) {
            sb.append(" AS ").append(alias);
        }
        return sb.toString();
    }
}
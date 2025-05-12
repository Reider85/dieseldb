package diesel;

class ConditionParseContext {
    final String normalizedCondition;
    final boolean isNot;

    ConditionParseContext(String normalizedCondition, boolean isNot) {
        this.normalizedCondition = normalizedCondition;
        this.isNot = isNot;
    }
}
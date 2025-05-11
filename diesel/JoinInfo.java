package diesel;

import java.util.ArrayList;
import java.util.List;

class JoinInfo {
    String tableName;
    String leftColumn;
    String rightColumn;
    String originalTable;
    QueryParser.JoinType joinType;
    List<Condition> onConditions;

    JoinInfo(String originalTable, String tableName, String leftColumn, String rightColumn, QueryParser.JoinType joinType) {
        this(originalTable, tableName, leftColumn, rightColumn, joinType, new ArrayList<>());
    }

    JoinInfo(String originalTable, String tableName, String leftColumn, String rightColumn, QueryParser.JoinType joinType, List<Condition> onConditions) {
        this.originalTable = originalTable;
        this.tableName = tableName;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.joinType = joinType;
        this.onConditions = onConditions;
    }

    @Override
    public String toString() {
        return "JoinInfo{originalTable=" + originalTable + ", table=" + tableName +
                ", leftColumn=" + leftColumn + ", rightColumn=" + rightColumn +
                ", joinType=" + joinType + ", onConditions=" + onConditions + "}";
    }
}
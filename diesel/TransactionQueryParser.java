package diesel;

class TransactionQueryParser {
    Query<?> parseTransactionQuery(String normalized) {
        if (normalized.equals("BEGIN TRANSACTION") ||
                normalized.startsWith("BEGIN TRANSACTION ISOLATION LEVEL")) {
            IsolationLevel isolationLevel = null;
            if (normalized.contains("ISOLATION LEVEL READ UNCOMMITTED")) {
                isolationLevel = IsolationLevel.READ_UNCOMMITTED;
            } else if (normalized.contains("ISOLATION LEVEL READ COMMITTED")) {
                isolationLevel = IsolationLevel.READ_COMMITTED;
            } else if (normalized.contains("ISOLATION LEVEL REPEATABLE READ")) {
                isolationLevel = IsolationLevel.REPEATABLE_READ;
            } else if (normalized.contains("ISOLATION LEVEL SERIALIZABLE")) {
                isolationLevel = IsolationLevel.SERIALIZABLE;
            }
            return new BeginTransactionQuery(isolationLevel);
        } else if (normalized.equals("COMMIT TRANSACTION")) {
            return new CommitTransactionQuery();
        } else if (normalized.equals("ROLLBACK TRANSACTION")) {
            return new RollbackTransactionQuery();
        } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")) {
            return new SetIsolationLevelQuery(IsolationLevel.READ_UNCOMMITTED);
        } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")) {
            return new SetIsolationLevelQuery(IsolationLevel.READ_COMMITTED);
        } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")) {
            return new SetIsolationLevelQuery(IsolationLevel.REPEATABLE_READ);
        } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")) {
            return new SetIsolationLevelQuery(IsolationLevel.SERIALIZABLE);
        }
        throw new IllegalArgumentException("Unsupported query type");
    }
}
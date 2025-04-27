package diesel;
import diesel.IsolationLevel;
import diesel.Table;
import diesel.TransactionQuery;

class BeginTransactionQuery implements TransactionQuery {
    private final IsolationLevel isolationLevel;

    public BeginTransactionQuery(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public String execute(Table table) {
        throw new UnsupportedOperationException("BeginTransactionQuery should be handled by Database directly");
    }
}
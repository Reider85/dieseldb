package diesel;

class RollbackTransactionQuery implements TransactionQuery {
    @Override
    public String execute(Table table) {
        throw new UnsupportedOperationException("RollbackTransactionQuery should be handled by Database directly");
    }
}
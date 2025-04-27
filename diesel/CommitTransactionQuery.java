package diesel;

class CommitTransactionQuery implements TransactionQuery {
    @Override
    public String execute(Table table) {
        throw new UnsupportedOperationException("CommitTransactionQuery should be handled by Database directly");
    }
}
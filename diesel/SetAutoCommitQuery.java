package diesel;

class SetAutoCommitQuery implements Query<String> {
    private final boolean autoCommit;

    public SetAutoCommitQuery(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public String execute(Table table) {
        throw new UnsupportedOperationException("SetAutoCommitQuery should be handled by Database directly");
    }
}

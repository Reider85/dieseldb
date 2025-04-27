package diesel;

class SetIsolationLevelQuery implements Query<String> {
    private final IsolationLevel isolationLevel;

    public SetIsolationLevelQuery(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public String execute(Table table) {
        throw new UnsupportedOperationException("SetIsolationLevelQuery should be handled by Database directly");
    }
}
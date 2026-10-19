package diesel;

/**
 * Executes a CREATE USER statement: creates an account with a salted password
 * hash through the database's user store (prompt4 #30, R3-008 step 1/4).
 *
 * @see Query
 * @see diesel.security.UserStore
 */
class CreateUserQuery implements Query<String> {
    private final String userName;
    private final String password;

    /**
     * @param userName the account name, case-preserved
     * @param password the plaintext password (hashed before storage)
     */
    public CreateUserQuery(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    /**
     * Returns the plaintext password. Only for parser/dispatch plumbing —
     * never log or persist it.
     */
    public String getPassword() {
        return password;
    }

    @Override
    public String execute(Table table) {
        return table.getDatabase().executeCreateUser(this);
    }
}

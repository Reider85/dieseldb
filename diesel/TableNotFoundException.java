package diesel;

/**
 * Thrown when a referenced table does not exist in the database.
 */
public class TableNotFoundException extends DieselException {

    public TableNotFoundException(String message) {
        super(message);
    }
}

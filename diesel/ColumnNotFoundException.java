package diesel;

/**
 * Thrown when a referenced column does not exist in a table.
 */
public class ColumnNotFoundException extends DieselException {

    public ColumnNotFoundException(String message) {
        super(message);
    }
}

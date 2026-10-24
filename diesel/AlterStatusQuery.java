package diesel;

import java.util.List;
import java.util.Map;

/**
 * Query DTO for {@code ALTER STATUS} (prompt4 #38, R3-010 step 2/3).
 * Returns one row per online ALTER (running and recently finished) with the
 * copy progress. The actual row assembly happens in
 * {@link Database#executeAlterStatus()} during dispatch; {@link #execute(Table)}
 * is never reached for this statement type.
 *
 * @see diesel.AlterTableCopyAlgorithm
 */
public final class AlterStatusQuery implements Query<List<Map<String, Object>>> {

    @Override
    public List<Map<String, Object>> execute(Table table) {
        // Handled by Database.dispatchInternal - never invoked with a table.
        return null;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o != null && getClass() == o.getClass());
    }

    @Override
    public int hashCode() {
        return AlterStatusQuery.class.hashCode();
    }

    @Override
    public String toString() {
        return "AlterStatusQuery{}";
    }
}

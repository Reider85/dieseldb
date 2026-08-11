package diesel;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A numeric sequence that auto-generates values for a column, e.g.
 * {@code SEQUENCE(users_seq 1 1)}. The generated type is either
 * {@link Long} or {@link Integer}; the current value advances atomically.
 *
 * @see Table#getSequences
 */
public class Sequence implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final Class<?> type;
    private final AtomicLong currentValue;
    private final long increment;

    /**
     * Creates a sequence starting at {@code startValue}.
     *
     * @param name        the sequence name
     * @param type        the generated type, must be {@code Long.class} or
     *                    {@code Integer.class}
     * @param startValue  the first value generated
     * @param increment   the step between consecutive values
     * @throws IllegalArgumentException if the type is neither Long nor Integer
     */
    public Sequence(String name, Class<?> type, long startValue, long increment) {
        if (!type.equals(Long.class) && !type.equals(Integer.class)) {
            throw new IllegalArgumentException("Sequence type must be Long or Integer");
        }
        this.name = name;
        this.type = type;
        this.currentValue = new AtomicLong(startValue - increment);
        this.increment = increment;
    }

    /**
     * Returns the sequence name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the generated value type.
     *
     * @return {@code Long.class} or {@code Integer.class}
     */
    public Class<?> getType() {
        return type;
    }

    /**
     * Returns the next value of the sequence, advancing the current value by
     * the configured increment.
     *
     * @return the next value as {@link Long} or {@link Integer}
     */
    public synchronized Object nextValue() {
        long next = currentValue.addAndGet(increment);
        return type.equals(Integer.class) ? (int) next : next;
    }

    /**
     * Returns the current (last generated) value of the sequence.
     *
     * @return the current value
     */
    public synchronized long getCurrentValue() {
        return currentValue.get();
    }
}
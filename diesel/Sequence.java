package diesel;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class Sequence implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final Class<?> type;
    private final AtomicLong currentValue;
    private final long increment;

    public Sequence(String name, Class<?> type, long startValue, long increment) {
        if (!type.equals(Long.class) && !type.equals(Integer.class)) {
            throw new IllegalArgumentException("Sequence type must be Long or Integer");
        }
        this.name = name;
        this.type = type;
        this.currentValue = new AtomicLong(startValue - increment);
        this.increment = increment;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public synchronized Object nextValue() {
        long next = currentValue.addAndGet(increment);
        return type.equals(Integer.class) ? (int) next : next;
    }

    public synchronized long getCurrentValue() {
        return currentValue.get();
    }
}
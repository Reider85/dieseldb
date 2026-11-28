package diesel.optimizer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class Histogram {
    public static final int DEFAULT_BUCKETS = 100;

    private Histogram() {
    }

    public static List<Object> equiDepth(List<Object> sortedValues, int bucketCount) {
        if (sortedValues == null || sortedValues.isEmpty()) {
            return List.of();
        }
        int size = sortedValues.size();
        int buckets = bucketCount > 0 ? Math.min(bucketCount, size) : Math.min(DEFAULT_BUCKETS, size);
        List<Object> bounds = new ArrayList<>(buckets + 1);
        bounds.add(sortedValues.get(0));
        for (int i = 1; i <= buckets; i++) {
            long index = Math.round((double) i * size / buckets);
            bounds.add(sortedValues.get((int) Math.min(size - 1L, index)));
        }
        return List.copyOf(bounds);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static int compare(Object left, Object right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        if (right == null) {
            return 1;
        }
        if (left.getClass().isInstance(right) && left instanceof Comparable) {
            return ((Comparable) left).compareTo(right);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    public static Comparator<Object> comparator() {
        return Histogram::compare;
    }
}

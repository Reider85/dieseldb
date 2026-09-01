package diesel;

import java.io.Serializable;
import java.util.BitSet;

/**
 * A simple Bloom filter for fast probabilistic membership tests.  Used by
 * {@link Table} to short-circuit index lookups when a key is certainly
 * <em>not</em> present, avoiding unnecessary B-tree traversals.
 *
 * <p>False positives are possible (the filter may say a key exists when it
 * does not); false negatives are impossible.
 *
 * <p>The filter auto-sizes based on the expected number of distinct keys and
 * the desired false-positive rate (1 % by default).
 */
class BloomFilter implements Serializable {
    private static final long serialVersionUID = 1L;
    private static int DEFAULT_NUM_HASHES = 7;
    private static double DEFAULT_FPP = 0.01;

    static {
        try {
            java.util.Properties props = new java.util.Properties();
            java.io.FileInputStream fis = new java.io.FileInputStream("config.properties");
            props.load(fis);
            fis.close();
            String val = props.getProperty("bloom.filter.hashes");
            if (val != null) DEFAULT_NUM_HASHES = Integer.parseInt(val.trim());
            String val2 = props.getProperty("bloom.filter.fpp");
            if (val2 != null) DEFAULT_FPP = Double.parseDouble(val2.trim());
        } catch (Exception ignored) {}
    }

    private final BitSet bits;
    private final int numHashes;
    private final int size;

    /**
     * Creates a Bloom filter sized for {@code expectedInsertions} distinct
     * keys with a false-positive probability of approximately {@code fpp}.
     *
     * @param expectedInsertions the expected number of distinct keys
     * @param fpp                the desired false-positive probability (0 &lt; fpp &lt; 1)
     */
    BloomFilter(int expectedInsertions, double fpp) {
        this.size = optimalSize(expectedInsertions, fpp);
        this.numHashes = optimalNumHashes(size, expectedInsertions);
        this.bits = new BitSet(size);
    }

    /**
     * Creates a Bloom filter with the default 1 % false-positive rate.
     *
     * @param expectedInsertions the expected number of distinct keys
     */
    BloomFilter(int expectedInsertions) {
        this(expectedInsertions, DEFAULT_FPP);
    }

    /** Adds {@code key} to the filter. */
    void put(Object key) {
        int hash1 = hash1(key);
        int hash2 = hash2(key);
        for (int i = 0; i < numHashes; i++) {
            bits.set(Math.abs((hash1 + i * hash2) % size));
        }
    }

    /**
     * Returns true when the filter <em>might</em> contain {@code key}.
     * A false positive is possible; a false negative is not.
     */
    boolean mightContain(Object key) {
        int hash1 = hash1(key);
        int hash2 = hash2(key);
        for (int i = 0; i < numHashes; i++) {
            if (!bits.get(Math.abs((hash1 + i * hash2) % size))) {
                return false;
            }
        }
        return true;
    }

    /** Clears all bits, resetting the filter to empty. */
    void clear() {
        bits.clear();
    }

    private static int hash1(Object key) {
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    private static int hash2(Object key) {
        int h = key.hashCode();
        return (h * 0x85ebca6b) ^ (h >>> 16);
    }

    private static int optimalSize(int n, double p) {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalNumHashes(int m, int n) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }
}

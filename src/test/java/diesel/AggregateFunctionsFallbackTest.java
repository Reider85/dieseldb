package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * TESTS-15: AggregateFunctions must produce correct results whether or not
 * jdk.incubator.vector is resolved. Profiles without
 * --add-modules=jdk.incubator.vector (fast, core, concurrency, network, all, ci)
 * run the scalar branch for every aggregate; perf, large, timing-quick, storage
 * and flaky profiles keep the flag and run the SIMD kernels. The property
 * diesel.aggregate.vector.disabled lets this test force the scalar branch in
 * either case, so both dispatch paths are asserted from a single profile.
 */
@Tag("query")
class AggregateFunctionsFallbackTest {

    @AfterEach
    void clearOverride() {
        System.clearProperty(AggregateFunctions.VECTOR_DISABLED_PROP);
    }

    @Test
    void forcedPropertyDisablesVectorDispatch() {
        System.setProperty(AggregateFunctions.VECTOR_DISABLED_PROP, "true");
        assertFalse(AggregateFunctions.useVector());
    }

    @Test
    void scalarFallbackComputesCorrectResults() {
        System.setProperty(AggregateFunctions.VECTOR_DISABLED_PROP, "true");
        assertFalse(AggregateFunctions.useVector());

        List<Number> ints = List.of(1, 2, 3, 4, 5);
        assertEquals(15L, AggregateFunctions.sumInt(ints));
        assertEquals(15L, AggregateFunctions.sumLong(ints));
        assertEquals(15.0, AggregateFunctions.sumFloat(ints), 1e-9);
        assertEquals(15.0, AggregateFunctions.sumDouble(ints), 1e-9);
        assertEquals(3.0, AggregateFunctions.average(ints), 1e-9);
        assertEquals(5L, AggregateFunctions.count(ints));
        assertEquals(1, AggregateFunctions.minInt(ints).intValue());
        assertEquals(5, AggregateFunctions.maxInt(ints).intValue());

        List<Number> withNull = new ArrayList<>(ints);
        withNull.add(null);
        assertEquals(5L, AggregateFunctions.count(withNull));
        assertEquals(3.0, AggregateFunctions.average(withNull), 1e-9);

        List<Number> empty = List.of();
        assertEquals(0L, AggregateFunctions.sumInt(empty));
        assertEquals(0L, AggregateFunctions.count(empty));
        assertNull(AggregateFunctions.average(empty));
    }

    @Test
    void defaultPathAndForcedScalarPathAgree() {
        List<Number> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            data.add(i % 7 == 0 ? null : i * 1.5);
        }

        double defaultSum = AggregateFunctions.sumDouble(data);
        Double defaultAvg = AggregateFunctions.average(data);

        System.setProperty(AggregateFunctions.VECTOR_DISABLED_PROP, "true");
        double scalarSum = AggregateFunctions.sumDouble(data);
        Double scalarAvg = AggregateFunctions.average(data);

        // Vector lanes and the scalar loop may sum doubles in different order,
        // so compare with a tolerance instead of bit equality.
        assertEquals(defaultSum, scalarSum, 1e-6);
        assertEquals(defaultAvg, scalarAvg, 1e-6);
        // sum of i*1.5 for i in [0,999] where i % 7 != 0:
        // (499500 - 7*(142*143/2)) * 1.5 = (499500 - 71071) * 1.5 = 428429 * 1.5
        assertEquals(642643.5, scalarSum, 1e-6);
        assertEquals(857L, AggregateFunctions.count(data));
    }
}

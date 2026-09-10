package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PerformanceQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleSelectWhereAge() {
        runSelectCount("PerformanceTest", "simple select where age",
                "SELECT NAME, AGE FROM USERS WHERE AGE < 30", 23);
    }

    @Test
    @Order(2)
    void simpleSelectClusteredIndex() {
        runSelectCount("PerformanceTest", "simple select clustered index",
                "SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50'", 1);
    }

    @Test
    @Order(3)
    void complexSelectAgeAndActive() {
        runSelectCount("PerformanceTest", "complex select age and active",
                "SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE", 11);
    }

    @Test
    @Order(4)
    void complexSelectParenthesizedOr() {
        runSelectCount("PerformanceTest", "complex select parenthesized or",
                "SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500", 17);
    }
}

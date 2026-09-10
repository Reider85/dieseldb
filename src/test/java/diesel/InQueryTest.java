package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class InQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleInOnBtreeIndex() {
        runSelectCount("InTest", "simple in on btree index",
                "SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52)", 3);
    }

    @Test
    @Order(2)
    void simpleInOnPrimaryKey() {
        runSelectCount("InTest", "simple in on primary key",
                "SELECT ID, NAME FROM USERS WHERE ID IN (50, 51, 52)", 3);
    }

    @Test
    @Order(3)
    void complexInWithAnd() {
        runSelectCount("InTest", "complex in with and",
                "SELECT ID, NAME FROM USERS WHERE NAME IN ('User50', 'User51', 'User52') AND BALANCE > 5000", 0);
    }

    @Test
    @Order(4)
    void complexInWithOr() {
        runSelectCount("InTest", "complex in with or",
                "SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE50', 'CODE51', 'CODE52') OR BALANCE > 5000", 3);
    }
}

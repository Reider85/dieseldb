package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OrderByQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleOrderByName() {
        runSelectCount("OrderByTest", "simple order by name",
                "SELECT ID, NAME FROM USERS ORDER BY NAME", recordCount());
    }

    @Test
    @Order(2)
    void simpleOrderByAgeDesc() {
        runSelectCount("OrderByTest", "simple order by age desc",
                "SELECT ID, AGE FROM USERS ORDER BY AGE DESC", recordCount());
    }
}

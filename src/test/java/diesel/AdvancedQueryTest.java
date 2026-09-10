package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AdvancedQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleSelectByPrimaryKey() {
        runSelectCount("AdvancedTest", "simple select by primary key",
                "SELECT ID, NAME FROM USERS WHERE ID = 50", 1);
    }

    @Test
    @Order(2)
    void simpleSelectByName() {
        runSelectCount("AdvancedTest", "simple select by name",
                "SELECT ID, NAME FROM USERS WHERE NAME = 'User50'", 1);
    }

    @Test
    @Order(3)
    void complexSelectMultiColumnAnd() {
        runSelectCount("AdvancedTest", "complex select with multi-column and conditions",
                "SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE50') AND (AGE = 50) AND (NAME = 'User50')", 0);
    }

    @Test
    @Order(4)
    void complexSelectOrLimitOffset() {
        runSelectCount("AdvancedTest", "complex select with or limit offset",
                "SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5", 0);
    }
}

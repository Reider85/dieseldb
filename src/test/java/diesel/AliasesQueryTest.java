package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AliasesQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleSelectWithAliasOrderBy() {
        runSelectCount("AliasesTest", "simple select with alias order by",
                "SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName", recordCount());
    }

    @Test
    @Order(2)
    void simpleSelectWithAsAliasOrderBy() {
        runSelectCount("AliasesTest", "simple select with as alias order by",
                "SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName", recordCount());
    }

    @Test
    @Order(3)
    void complexSelectMinMaxAvgWithJoinGroupBy() {
        runSelectCount("AliasesTest", "complex select min max avg with join and group by",
                "SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge " +
                        "FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                        "GROUP BY userName, transDate ORDER BY transDate DESC", recordCount());
    }

    @Test
    @Order(4)
    void complexSelectMultipleInnerJoins() {
        runSelectCount("AliasesTest", "complex select with multiple inner joins",
                "SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName " +
                        "FROM USERS u " +
                        "INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID " +
                        "INNER JOIN USERS u2 ON u.ID = u2.ID " +
                        "LIMIT 10 OFFSET 5", 10);
    }
}

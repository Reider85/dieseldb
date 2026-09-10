package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GroupByQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleGroupByMinMaxAvg() {
        runSelectCount("GroupByTest", "simple group by min max avg",
                "SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME", recordCount());
    }

    @Test
    @Order(2)
    void simpleGroupBySumCount() {
        runSelectCount("GroupByTest", "simple group by sum count",
                "SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME", recordCount());
    }

    @Test
    @Order(3)
    void complexGroupByDateHaving() {
        runSelectCount("GroupByTest", "complex group by date having",
                "SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0", recordCount());
    }

    @Test
    @Order(4)
    void complexGroupByJoinStringDate() {
        runSelectCount("GroupByTest", "complex group by join string date",
                "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) " +
                        "FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID " +
                        "GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC", recordCount());
    }
}

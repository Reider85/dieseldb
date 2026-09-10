package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SubqueryQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleSubqueryInInClause() {
        runSelectCount("SubqueriesTest", "simple subquery in in clause",
                "SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10", 10);
    }

    @Test
    @Order(2)
    void simpleSubqueryInWhere() {
        runSelectCount("SubqueriesTest", "simple subquery in where",
                "SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) LIMIT 10", 10);
    }

    @Test
    @Order(3)
    void complexSubqueryInColumnWhereGroupByHaving() {
        runSelectCount("SubqueriesTest", "complex subquery in column where group by having",
                "SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count " +
                        "FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) " +
                        "GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) " +
                        "HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10", 0);
    }

    @Test
    @Order(4)
    void complexSubqueryInColumnInnerJoinOn() {
        runSelectCount("SubqueriesTest", "complex subquery in column inner join on",
                "SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name " +
                        "FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) LIMIT 10", 10);
    }
}

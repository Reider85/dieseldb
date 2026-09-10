package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JoinQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleInnerJoinOnPrimaryKey() {
        runSelectCount("JoinTest", "simple inner join on primary key",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                        "WHERE USERS.ID IN (50, 51, 52)", 3);
    }

    @Test
    @Order(2)
    void simpleInnerJoinOnNonIndexed() {
        runSelectCount("JoinTest", "simple inner join on non indexed field",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE " +
                        "WHERE USERS.BALANCE = 5100.00", 0);
    }

    @Test
    @Order(3)
    void complexFullJoinOnPrimaryKey() {
        runSelectCount("JoinTest", "complex full join on primary key",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID " +
                        "WHERE USERS.ID IN (50, 51, 52)", 3);
    }

    @Test
    @Order(4)
    void complexInnerJoinWithAndOrInOn() {
        runSelectCount("JoinTest", "complex inner join with and or in on",
                "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO " +
                        "FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) " +
                        "WHERE USERS.ID IN (50, 51, 52)", 3);
    }
}

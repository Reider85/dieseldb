package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LikeQueryTest extends AbstractDieselTest {

    @Test
    @Order(1)
    void simpleLikeOnName() {
        runSelectCount("LikeTest", "simple like on name",
                "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er50' AND NAME LIKE '%User50%' AND NAME LIKE 'User50%'", 1);
    }

    @Test
    @Order(2)
    void simpleLikeOnUserCode() {
        runSelectCount("LikeTest", "simple like on user code",
                "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE50' AND USER_CODE LIKE '%CODE50%' AND USER_CODE LIKE 'CODE50%'", 1);
    }

    @Test
    @Order(3)
    void complexLikeWithAnd() {
        runSelectCount("LikeTest", "complex like with and",
                "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er50' AND NAME LIKE '%User50%' AND NAME LIKE 'User50%' AND BALANCE > 5000", 0);
    }

    @Test
    @Order(4)
    void complexLikeWithOr() {
        runSelectCount("LikeTest", "complex like with or",
                "SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE50' AND USER_CODE LIKE '%CODE50%' AND USER_CODE LIKE 'CODE50%' OR BALANCE > 5000", 1);
    }
}

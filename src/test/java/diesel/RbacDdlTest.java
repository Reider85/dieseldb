package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import diesel.security.PasswordHasher;
import diesel.security.Role;
import diesel.security.UserExistsException;
import diesel.security.UserNotFoundException;
import diesel.security.RoleNotFoundException;
import diesel.security.UserStore;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.AfterEach;

/**
 * Prompt 4 #30 acceptance: CREATE USER / CREATE ROLE / GRANT SQL end to end
 * through {@link Database#executeQuery}, including system-table DROP
 * protection and restart persistence.
 */
@Tag("smoke")
class RbacDdlTest {

    private Database database;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
    }


    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
            database = null;
        }
    }
    @Test
    void createUserSqlStoresHashedPassword() {
        Object result = database.executeQuery("CREATE USER alice PASSWORD 'secret'", null);
        assertEquals("User created: alice", result);

        UserStore store = database.getUserStore();
        assertTrue(store.userExists("alice"));
        String stored = store.findUser("alice").orElseThrow().getPasswordHash();
        assertFalse(stored.contains("secret"), "password must not be stored in plaintext");
        assertTrue(PasswordHasher.isEncodedHash(stored));
        assertTrue(store.getPasswordHasher().verify("secret", stored));
    }

    @Test
    void duplicateCreateUserSqlThrows() {
        database.executeQuery("CREATE USER alice PASSWORD 'secret'", null);
        assertThrows(UserExistsException.class,
                () -> database.executeQuery("CREATE USER alice PASSWORD 'other'", null));
    }

    @Test
    void passwordCaseIsPreserved() {
        database.executeQuery("CREATE USER bob PASSWORD 'SecReT123'", null);
        UserStore store = database.getUserStore();
        String stored = store.findUser("bob").orElseThrow().getPasswordHash();
        assertTrue(store.getPasswordHasher().verify("SecReT123", stored));
        assertFalse(store.getPasswordHasher().verify("secret123", stored));
    }

    @Test
    void createRoleAndGrantSql() {
        database.executeQuery("CREATE USER alice PASSWORD 'secret'", null);
        Object role = database.executeQuery("CREATE ROLE analyst", null);
        assertEquals("Role created: analyst", role);

        Object grant = database.executeQuery("GRANT analyst TO alice", null);
        assertEquals("Role granted: analyst -> alice", grant);

        List<Role> roles = database.getUserStore().rolesOfUser("alice");
        assertEquals(1, roles.size());
        assertEquals("analyst", roles.get(0).getName());
    }

    @Test
    void grantUnknownRoleThrows() {
        database.executeQuery("CREATE USER alice PASSWORD 'secret'", null);
        assertThrows(RoleNotFoundException.class,
                () -> database.executeQuery("GRANT ghost TO alice", null));
    }

    @Test
    void grantUnknownUserThrows() {
        database.executeQuery("CREATE ROLE analyst", null);
        assertThrows(UserNotFoundException.class,
                () -> database.executeQuery("GRANT analyst TO ghost", null));
    }

    @Test
    void malformedCreateUserSqlThrowsSyntaxError() {
        assertThrows(SyntaxErrorException.class,
                () -> database.executeQuery("CREATE USER alice", null));
        assertThrows(SyntaxErrorException.class,
                () -> database.executeQuery("CREATE USER alice PASSWORD secret", null));
    }

    @Test
    void malformedGrantSqlThrowsSyntaxError() {
        assertThrows(SyntaxErrorException.class,
                () -> database.executeQuery("GRANT analyst", null));
    }

    @Test
    void systemTablesCannotBeDropped() {
        database.executeQuery("CREATE USER alice PASSWORD 'secret'", null);
        assertThrows(IllegalArgumentException.class, () -> database.dropTable("DIESEL_USERS"));
        assertThrows(IllegalArgumentException.class, () -> database.dropTable("diesel_roles"));
        assertTrue(database.getUserStore().userExists("alice"), "user survives the drop attempt");
    }

    @Test
    void usersSurviveRestart() {
        database.executeQuery("CREATE USER alice PASSWORD 'secret'", null);
        database.executeQuery("CREATE ROLE analyst", null);
        database.executeQuery("GRANT analyst TO alice", null);
        database.saveTablesToDisk();

        Database reopened = new Database();
        reopened.setDataDir(tempDir.toString());
        reopened.loadTablesFromDisk();
        UserStore store = reopened.getUserStore();
        assertTrue(store.userExists("alice"));
        String stored = store.findUser("alice").orElseThrow().getPasswordHash();
        assertTrue(store.getPasswordHasher().verify("secret", stored));
        assertEquals(1, store.rolesOfUser("alice").size());
    }
}

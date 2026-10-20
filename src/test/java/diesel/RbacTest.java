package diesel;

import static org.junit.jupiter.api.Assertions.*;
import diesel.security.Authenticator;
import diesel.security.UserStore;
import diesel.security.Session;
import diesel.security.AccessDeniedException;
import diesel.security.Privilege;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests RBAC privilege enforcement (prompt4 #32).
 * 
 * <p>Tests run against an embedded database with authentication enabled.
 * Sessions are created via the Authenticator to simulate the server flow.
 * Store-level grants use the UserStore API; table creation for non-CREATE
 * tests uses the session-less executeQuery overload (embedded bypass).
 */
@Tag("smoke")
public class RbacTest {
    
    @TempDir Path tempDir;
    private Database database;
    private Authenticator authenticator;
    private UserStore userStore;
    
    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
        userStore = database.getUserStore();
        authenticator = new Authenticator(userStore);
    }
    
    @AfterEach
    void tearDown() {
        database.close();
    }
    
    /**
     * Ensures the admin account exists with its forced-change bootstrap
     * password, performs the first-login password change, and returns the
     * fully authenticated admin session.
     */
    private Session loginAdmin() {
        userStore.resetAdminPassword();
        Session tempSession = authenticator.authenticate("admin", UserStore.DEFAULT_ADMIN_PASSWORD);
        database.executeQuery("ALTER USER admin PASSWORD 'adminpass'", null, tempSession);
        return authenticator.authenticate("admin", "adminpass");
    }
    
    @Test
    void userWithoutSelectPrivilegeOnTable_AccessDenied() {
        // Create user and role but no SELECT privilege
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        // No grant of SELECT privilege
        
        // The target table must exist: SELECT resolves its table during parse,
        // so a missing table would raise TableNotFound before the privilege check
        database.executeQuery("CREATE TABLE users (ID LONG, NAME STRING)", null);
        
        // Authenticate as alice
        Session session = authenticator.authenticate("alice", "password");
        assertNotNull(session);
        
        // Try to SELECT from table - should be denied before execution
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID, NAME FROM USERS", null, session);
        }, "Access denied: user 'alice' has no SELECT privilege on users");
    }
    
    @Test
    void userWithSelectPrivilegeOnTable_Allowed() {
        // Create user and role with SELECT + INSERT privileges
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test");
        userStore.grantPrivilege("analyst", Privilege.Action.INSERT, "test");
        
        // Table creation is embedded setup (alice has no CREATE privilege)
        database.executeQuery("CREATE TABLE test (ID LONG, NAME STRING)", null);
        
        // Authenticate as alice
        Session session = authenticator.authenticate("alice", "password");
        assertNotNull(session);
        
        // Alice can INSERT and SELECT within her granted privileges
        database.executeQuery("INSERT INTO test (ID, NAME) VALUES (1, 'Alice')", null, session);
        
        List<Map<String, Object>> result = (List<Map<String, Object>>) 
            database.executeQuery("SELECT ID, NAME FROM test", null, session);
        assertEquals(1, result.size());
        assertEquals("Alice", result.get(0).get("NAME"));
    }
    
    @Test
    void mustChangePassword_BlocksAllQueriesExceptAlterUser() {
        // resetAdminPassword creates the admin with the forced-change flag
        userStore.resetAdminPassword();
        Session adminSession = authenticator.authenticate("admin", UserStore.DEFAULT_ADMIN_PASSWORD);
        assertTrue(adminSession.isMustChangePassword(), "bootstrap admin must be flagged");
        
        // All non-ALTER statements are hard-blocked before execution
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM DIESEL_USERS", null, adminSession);
        });
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("CREATE TABLE t1 (ID INTEGER)", null, adminSession);
        });
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("INSERT INTO t1 (ID) VALUES (1)", null, adminSession);
        });
        
        // Cross-account ALTER USER stays denied while the flag is pending
        userStore.createUser("bob", "password");
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("ALTER USER bob PASSWORD 'hacked'", null, adminSession);
        });
        
        // Own-account ALTER USER is the one allowed statement
        assertDoesNotThrow(() -> {
            database.executeQuery("ALTER USER admin PASSWORD 'adminpass'", null, adminSession);
        });
        assertFalse(adminSession.isMustChangePassword(), "flag clears on the live session");
        
        // A fresh session with the new password works
        Session fresh = authenticator.authenticate("admin", "adminpass");
        assertFalse(fresh.isMustChangePassword());
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE t1 (ID INTEGER)", null, fresh);
        });
    }
    
    @Test
    void adminUserCanDoAnything() {
        // Admin with changed password has ADMIN privilege implicitly
        Session session = loginAdmin();
        assertFalse(session.isMustChangePassword());
        
        // Should be able to create tables, insert, select, etc.
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE admin_test (ID INTEGER)", null, session);
        });
        
        assertDoesNotThrow(() -> {
            database.executeQuery("INSERT INTO admin_test (ID) VALUES (1)", null, session);
        });
        
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM admin_test", null, session);
        });
        
        // Should be able to GRANT privileges (role must exist)
        userStore.createRole("analyst");
        assertDoesNotThrow(() -> {
            database.executeQuery("GRANT SELECT ON admin_test TO analyst", null, session);
        });
        
        // Should be able to ALTER other users (cross-account via ADMIN)
        userStore.createUser("bob", "password");
        assertDoesNotThrow(() -> {
            database.executeQuery("ALTER USER bob PASSWORD 'newpass'", null, session);
        });
    }
    
    @Test
    void nonAdminCannotAlterOtherUsers() {
        userStore.createUser("alice", "password");
        userStore.createUser("bob", "password");
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Alice cannot change Bob's password
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("ALTER USER bob PASSWORD 'hacked'", null, aliceSession);
        }, "Access denied: user 'alice' has no ADMIN privilege on database");
    }
    
    @Test
    void revokePrivilege_DeniesAccess() {
        // Setup user with privilege
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        
        // Table creation is embedded setup (alice has no CREATE privilege)
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Initially should be able to SELECT
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
        
        // Revoke the privilege (change counter invalidates the session cache)
        userStore.revokePrivilege("analyst", Privilege.Action.SELECT, "test_table");
        
        // Now should be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        }, "Access denied: user 'alice' has no SELECT privilege on test_table");
    }
    
    @Test
    void databaseWidePrivilege() {
        // Grant SELECT database-wide
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, ""); // empty = DB-wide
        
        // Table creation is embedded setup (alice has no CREATE privilege)
        database.executeQuery("CREATE TABLE table1 (ID INTEGER)", null);
        database.executeQuery("CREATE TABLE table2 (ID INTEGER)", null);
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Should be able to SELECT from any table
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM table1", null, session);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM table2", null, session);
        });
    }
    
    @Test
    void adminPrivilegeIncludesAll() {
        // Grant ADMIN database-wide
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.ADMIN, "");
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Should be able to do anything
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE admin_test (ID INTEGER)", null, session);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("INSERT INTO admin_test (ID) VALUES (1)", null, session);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM admin_test", null, session);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("DROP TABLE admin_test", null, session);
        });
    }
}

package diesel;

import static org.junit.jupiter.api.Assertions.*;
import diesel.security.Authenticator;
import diesel.security.UserStore;
import diesel.security.Session;
import diesel.security.AccessDeniedException;
import diesel.security.Privilege;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests GRANT and REVOKE privilege operations (prompt4 #32).
 * 
 * <p>Tests run against an embedded database with authentication enabled.
 * GRANT/REVOKE statements issued with a session require ADMIN (the admin
 * session covers that path); pure grant-effect tests use the session-less
 * executeQuery overload as embedded setup.
 */
@Tag("smoke")
public class GrantTest {
    
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
    void grantSelect_AllowsSelect() {
        // Setup: user and role without privileges
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Admin creates the test table (must-change flow handled by helper)
        Session adminSession = loginAdmin();
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null, adminSession);
        
        // Initially should be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
        
        // Grant SELECT via SQL from the admin session
        database.executeQuery("GRANT SELECT ON test_table TO analyst", null, adminSession);
        
        // Now should be allowed
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
    }
    
    @Test
    void grantMultipleActions() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        
        // Embedded setup: table + multi-action grant
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        database.executeQuery("GRANT SELECT, INSERT ON test_table TO analyst", null);
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Should be able to SELECT and INSERT
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("INSERT INTO test_table (ID) VALUES (1)", null, session);
        });
        
        // But not UPDATE
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("UPDATE test_table SET ID = 2", null, session);
        });
    }
    
    @Test
    void grantAdmin_AllowsEverything() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        
        // Embedded setup: ADMIN grant (admin is a meta-privilege)
        database.executeQuery("GRANT ADMIN TO analyst", null);
        
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
    
    @Test
    void revokePrivilege_DeniesAccess() {
        // Setup: grant privilege first
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Initially should be allowed
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
        
        // Revoke the privilege via SQL (embedded setup path)
        database.executeQuery("REVOKE SELECT ON test_table FROM analyst", null);
        
        // Now should be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        }, "Access denied: user 'alice' has no SELECT privilege on test_table");
    }
    
    @Test
    void revokeMultipleActions() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        userStore.grantPrivilege("analyst", Privilege.Action.INSERT, "test_table");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Initially can both SELECT and INSERT
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("INSERT INTO test_table (ID) VALUES (1)", null, session);
        });
        
        // Revoke both
        database.executeQuery("REVOKE SELECT, INSERT ON test_table FROM analyst", null);
        
        // Now neither should work
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("INSERT INTO test_table (ID) VALUES (2)", null, session);
        });
    }
    
    @Test
    void revokeNonexistentPrivilege_NoOp() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        // Table must exist for SELECT to reach the privilege check
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Revoke nonexistent privilege - should not error
        assertDoesNotThrow(() -> {
            database.executeQuery("REVOKE SELECT ON test_table FROM analyst", null);
        });
        
        // Still should be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
    }
    
    @Test
    void grantDatabaseWidePrivilege() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        
        // Embedded setup: SELECT database-wide (no ON clause) + tables
        database.executeQuery("GRANT SELECT TO analyst", null);
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
    void revokeDatabaseWidePrivilege() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, ""); // DB-wide
        database.executeQuery("CREATE TABLE table1 (ID INTEGER)", null);
        
        Session session = authenticator.authenticate("alice", "password");
        
        // Initially can SELECT from anywhere
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM table1", null, session);
        });
        
        // Revoke database-wide privilege
        database.executeQuery("REVOKE SELECT FROM analyst", null);
        
        // Now should be denied everywhere
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT * FROM table1", null, session);
        });
    }
    
    @Test
    void grantToNonexistentRole_Error() {
        userStore.createUser("alice", "password");
        
        // Grant to nonexistent role should fail (embedded - no privilege check
        // is involved, the role lookup itself must throw)
        assertThrows(Exception.class, () -> {
            database.executeQuery("GRANT SELECT ON test_table TO nonexistent_role", null);
        });
    }
    
    @Test
    void grantDuplicatePrivilege_NoOp() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        // Grant same privilege twice - should not error
        assertDoesNotThrow(() -> {
            database.executeQuery("GRANT SELECT ON test_table TO analyst", null);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("GRANT SELECT ON test_table TO analyst", null);
        });
        
        // Should still work
        Session session = authenticator.authenticate("alice", "password");
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, session);
        });
    }
}

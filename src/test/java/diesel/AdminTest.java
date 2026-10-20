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
 * Tests ADMIN privilege and admin user capabilities (prompt4 #32).
 * 
 * <p>Tests run against an embedded database with authentication enabled.
 * Statements executed through {@code database.executeQuery(..., session)}
 * are privilege-checked; pure store setup uses the UserStore API or the
 * session-less executeQuery overload.
 */
@Tag("smoke")
public class AdminTest {
    
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
    void adminUserCanGrantAndRevokePrivileges() {
        // Setup: users and roles (store-level, no SQL needed)
        userStore.createUser("alice", "password");
        userStore.createUser("bob", "password");
        userStore.createRole("analyst");
        userStore.createRole("viewer");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantRoleToUser("bob", "viewer");
        
        // The target table must exist before SELECT against it
        database.executeQuery("CREATE TABLE sensitive_data (ID INTEGER, NAME STRING)", null);
        database.executeQuery("INSERT INTO sensitive_data (ID, NAME) VALUES (1, 'secret')", null);
        
        Session adminSession = loginAdmin();
        
        // Admin can grant privileges
        assertDoesNotThrow(() -> {
            database.executeQuery("GRANT SELECT ON sensitive_data TO analyst", null, adminSession);
        });
        
        // Alice (analyst) can now SELECT
        Session aliceSession = authenticator.authenticate("alice", "password");
        List<Map<String, Object>> allowed = (List<Map<String, Object>>)
            database.executeQuery("SELECT ID, NAME FROM sensitive_data", null, aliceSession);
        assertEquals(1, allowed.size());
        assertEquals("secret", allowed.get(0).get("NAME"));
        
        // Admin can revoke privileges
        assertDoesNotThrow(() -> {
            database.executeQuery("REVOKE SELECT ON sensitive_data FROM analyst", null, adminSession);
        });
        
        // Alice can no longer SELECT
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID, NAME FROM sensitive_data", null, aliceSession);
        });
    }
    
    @Test
    void adminUserCanCreateAndDropTables() {
        Session adminSession = loginAdmin();
        
        // Admin can create tables
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE admin_table (ID INTEGER, NAME STRING)", null, adminSession);
        });
        
        // Admin can insert data
        assertDoesNotThrow(() -> {
            database.executeQuery("INSERT INTO admin_table (ID, NAME) VALUES (1, 'Admin')", null, adminSession);
        });
        
        // Admin can select data
        List<Map<String, Object>> result = (List<Map<String, Object>>) 
            database.executeQuery("SELECT NAME FROM admin_table", null, adminSession);
        assertEquals(1, result.size());
        assertEquals("Admin", result.get(0).get("NAME"));
        
        // Admin can drop tables
        assertDoesNotThrow(() -> {
            database.executeQuery("DROP TABLE admin_table", null, adminSession);
        });
    }
    
    @Test
    void adminUserCanCreateUsersAndRoles() {
        Session adminSession = loginAdmin();
        
        // Admin can create roles
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE ROLE manager", null, adminSession);
        });
        
        // Admin can create users
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE USER manager_user PASSWORD 'managerpass'", null, adminSession);
        });
        
        // Admin can grant roles to users
        assertDoesNotThrow(() -> {
            database.executeQuery("GRANT manager TO manager_user", null, adminSession);
        });
        
        // New user can authenticate
        Session managerSession = authenticator.authenticate("manager_user", "managerpass");
        assertNotNull(managerSession);
    }
    
    @Test
    void adminUserCanAlterAnyUser() {
        userStore.createUser("alice", "password");
        Session adminSession = loginAdmin();
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Admin can change Alice's password (cross-account, requires ADMIN)
        assertDoesNotThrow(() -> {
            database.executeQuery("ALTER USER alice PASSWORD 'newpass123'", null, adminSession);
        });
        
        // Alice can authenticate with new password
        Session newAliceSession = authenticator.authenticate("alice", "newpass123");
        assertNotNull(newAliceSession);
        
        // Old password no longer works
        assertThrows(Exception.class, () -> {
            authenticator.authenticate("alice", "password");
        });
    }
    
    @Test
    void adminUserHasAllPrivilegesImplicitly() {
        Session adminSession = loginAdmin();
        
        // Admin should be able to do anything without explicit grants
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE anything (ID INTEGER)", null, adminSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("INSERT INTO anything (ID) VALUES (1)", null, adminSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM anything", null, adminSession);
        });
        
        // Even without any explicit privileges
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM anything", null, adminSession);
        });
    }
    
    @Test
    void adminUserCanGrantAdminPrivilege() {
        userStore.createUser("alice", "password");
        userStore.createRole("super_user");
        userStore.createRole("viewer");
        userStore.grantRoleToUser("alice", "super_user");
        
        Session adminSession = loginAdmin();
        
        // Grant ADMIN privilege to the role
        assertDoesNotThrow(() -> {
            database.executeQuery("GRANT ADMIN TO super_user", null, adminSession);
        });
        
        // Alice (super_user) now has admin privileges
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Alice can now do admin-like operations
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE alice_admin_test (ID INTEGER)", null, aliceSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("GRANT SELECT ON alice_admin_test TO viewer", null, aliceSession);
        });
    }
    
    @Test
    void adminUserCanAccessSystemTables() {
        Session adminSession = loginAdmin();
        
        // Admin can query system tables (they have ADMIN privilege)
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT NAME FROM DIESEL_USERS", null, adminSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT NAME FROM DIESEL_ROLES", null, adminSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ACTION, TABLE_NAME FROM DIESEL_PRIVILEGES", null, adminSession);
        });
    }
    
    @Test
    void nonAdminCannotAccessSystemTables() {
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "regular_table");
        userStore.grantPrivilege("analyst", Privilege.Action.INSERT, "regular_table");
        userStore.grantPrivilege("analyst", Privilege.Action.CREATE, "");
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Alice can access regular tables
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE regular_table (ID INTEGER)", null, aliceSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM regular_table", null, aliceSession);
        });
        
        // But cannot access system tables (no SELECT on diesel_*)
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT NAME FROM DIESEL_USERS", null, aliceSession);
        }, "Access denied: user 'alice' has no SELECT privilege on diesel_users");
    }
}

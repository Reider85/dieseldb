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
 * Tests privilege cache invalidation (prompt4 #32).
 * 
 * <p>Verifies that GRANT and REVOKE operations immediately affect
 * active sessions without requiring re-authentication. SQL GRANT/REVOKE
 * statements run through the session-less executeQuery overload (embedded
 * setup - who issues the grant is covered by GrantTest/AdminTest).
 */
@Tag("smoke")
public class CacheInvalidationTest {
    
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
    
    @Test
    void grantPrivilegeImmediatelyVisibleInActiveSession() {
        // Setup: create user and role but no privileges
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        }, "Access denied: user 'alice' has no SELECT privilege on test_table");
        
        // Grant privilege through user store (simulating internal operation)
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        
        // Now Alice should be allowed - cache should be invalidated
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
    }
    
    @Test
    void revokePrivilegeImmediatelyDeniedInActiveSession() {
        // Setup: grant privilege first
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should be allowed
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
        
        // Revoke privilege
        userStore.revokePrivilege("analyst", Privilege.Action.SELECT, "test_table");
        
        // Now Alice should be denied - cache should be invalidated
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        }, "Access denied: user 'alice' has no SELECT privilege on test_table");
    }
    
    @Test
    void grantRoleImmediatelyVisibleInActiveSession() {
        // Setup: create user and role but no grant
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.createRole("viewer");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should have no privileges
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
        
        // Grant role to user
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        
        // Now Alice should be allowed - cache should be invalidated
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
    }
    
    @Test
    void sqlGrantInvalidatesCache() {
        // Setup: create user and role
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
        
        // Grant privilege via SQL
        database.executeQuery("GRANT SELECT ON test_table TO analyst", null);
        
        // Cache should be invalidated and Alice should now be allowed
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
    }
    
    @Test
    void sqlRevokeInvalidatesCache() {
        // Setup: grant privilege first
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should be allowed
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
        
        // Revoke privilege via SQL
        database.executeQuery("REVOKE SELECT ON test_table FROM analyst", null);
        
        // Cache should be invalidated and Alice should now be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        }, "Access denied: user 'alice' has no SELECT privilege on test_table");
    }
    
    @Test
    void multipleSessionsSeeGrantImmediately() {
        // Setup: create user and role
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession1 = authenticator.authenticate("alice", "password");
        Session aliceSession2 = authenticator.authenticate("alice", "password");
        
        // Both sessions should initially be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession1);
        });
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession2);
        });
        
        // Grant privilege via SQL
        database.executeQuery("GRANT SELECT ON test_table TO analyst", null);
        
        // Both sessions should now be allowed (cache keyed off change counter)
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession1);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession2);
        });
    }
    
    @Test
    void grantRoleToUserInvalidatesCache() {
        // Setup: create user and role, grant privilege to role
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.createRole("viewer");
        userStore.grantPrivilege("viewer", Privilege.Action.SELECT, "test_table");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should have no privileges (not in viewer role)
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
        
        // Grant role to user
        database.executeQuery("GRANT viewer TO alice", null);
        
        // Cache should be invalidated and Alice should now be allowed
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
    }
    
    @Test
    void revokeRoleInvalidatesCache() {
        // Setup: create user and role, grant role and privilege
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        userStore.grantPrivilege("analyst", Privilege.Action.SELECT, "test_table");
        database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null);
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should be allowed
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        });
        
        // Revoke role (no SQL syntax for role revocation - store API)
        userStore.revokeRoleFromUser("alice", "analyst");
        
        // Cache should be invalidated and Alice should now be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT ID FROM test_table", null, aliceSession);
        }, "Access denied: user 'alice' has no SELECT privilege on test_table");
    }
    
    @Test
    void adminPrivilegeGrantImmediatelyGrantsAllAccess() {
        // Setup: create user and role
        userStore.createUser("alice", "password");
        userStore.createRole("analyst");
        userStore.grantRoleToUser("alice", "analyst");
        
        Session aliceSession = authenticator.authenticate("alice", "password");
        
        // Initially Alice should be denied
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null, aliceSession);
        });
        assertThrows(AccessDeniedException.class, () -> {
            database.executeQuery("SELECT * FROM DIESEL_USERS", null, aliceSession);
        });
        
        // Grant ADMIN privilege
        database.executeQuery("GRANT ADMIN TO analyst", null);
        
        // Cache should be invalidated and Alice should now be able to do anything
        assertDoesNotThrow(() -> {
            database.executeQuery("CREATE TABLE test_table (ID INTEGER)", null, aliceSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("INSERT INTO test_table (ID) VALUES (1)", null, aliceSession);
        });
        assertDoesNotThrow(() -> {
            database.executeQuery("SELECT * FROM test_table", null, aliceSession);
        });
    }
}

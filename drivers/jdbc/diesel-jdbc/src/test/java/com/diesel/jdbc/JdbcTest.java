package com.diesel.jdbc;

import diesel.LibpqTestServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 55 acceptance: DriverManager.getConnection("jdbc:diesel://...") works
 * end-to-end against an in-process LibpqServer.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JdbcTest {

    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpass";

    @TempDir
    static Path tempDir;

    private LibpqTestServer server;

    @BeforeAll
    void startServer() throws Exception {
        server = LibpqTestServer.start(tempDir);
    }

    @AfterAll
    void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    /** DROP TABLE in DieselDB has no IF EXISTS clause; ignore missing tables. */
    private static void dropIfExists(Statement stmt, String table) {
        try {
            stmt.executeUpdate("DROP TABLE " + table);
        } catch (SQLException ignore) {
            // table did not exist yet
        }
    }

    private Connection open() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", TEST_USER);
        props.setProperty("password", TEST_PASSWORD);
        return DriverManager.getConnection(server.jdbcUrl(), props);
    }

    // ---- acceptance: DriverManager.getConnection works ----

    @Test
    void driverIsRegisteredWithDriverManager() throws SQLException {
        assertNotNull(DriverManager.getDriver(server.jdbcUrl()));
        assertTrue(DriverManager.getDriver(server.jdbcUrl()) instanceof DieselDriver);
    }

    @Test
    void acceptsUrlMatchesPrefix() throws SQLException {
        DieselDriver driver = new DieselDriver();
        assertTrue(driver.acceptsURL("jdbc:diesel://localhost:5432/db"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/db"));
        // Non-matching URL must return null per JDBC spec, not throw.
        assertTrue(driver.connect("jdbc:postgresql://localhost:5432/db", new Properties()) == null);
    }

    @Test
    void connectAndBasicSelect() throws SQLException {
        try (Connection conn = open()) {
            assertFalse(conn.isClosed());
            assertTrue(conn.isValid(5));

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void fullCrudLifecycle() throws SQLException {
        try (Connection conn = open()) {
            try (Statement stmt = conn.createStatement()) {
                dropIfExists(stmt, "jdbc_demo");
                stmt.executeUpdate("CREATE TABLE jdbc_demo (ID INTEGER, NAME STRING, AGE INTEGER)");
            }

            // INSERT via PreparedStatement
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO jdbc_demo (ID, NAME, AGE) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "Alice");
                ps.setInt(3, 25);
                assertEquals(1, ps.executeUpdate());

                ps.setInt(1, 2);
                ps.setString(2, "Bob");
                ps.setInt(3, 30);
                assertEquals(1, ps.executeUpdate());
            }

            // SELECT both rows back
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT ID, NAME, AGE FROM jdbc_demo")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Alice", rs.getString(2));
                assertEquals(25, rs.getInt(3));

                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals("Bob", rs.getString(2));
                assertEquals(30, rs.getInt(3));

                assertFalse(rs.next());
            }

            // SELECT with WHERE through a prepared statement
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT NAME FROM jdbc_demo WHERE ID = ?")) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("Bob", rs.getString(1));
                    assertFalse(rs.next());
                }
            }

            // UPDATE
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE jdbc_demo SET AGE = ? WHERE ID = ?")) {
                ps.setInt(1, 26);
                ps.setInt(2, 1);
                assertEquals(1, ps.executeUpdate());
            }

            // verify update
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT AGE FROM jdbc_demo WHERE ID = 1")) {
                assertTrue(rs.next());
                assertEquals(26, rs.getInt(1));
                assertFalse(rs.next());
            }

            // DELETE
            try (Statement stmt = conn.createStatement()) {
                assertEquals(1, stmt.executeUpdate("DELETE FROM jdbc_demo WHERE ID = 1"));
            }

            // verify delete: one row remains
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM jdbc_demo")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next());
            }

            // cleanup
            try (Statement stmt = conn.createStatement()) {
                dropIfExists(stmt, "jdbc_demo");
            }
        }
    }

    @Test
    void columnLabelsResolveByName() throws SQLException {
        try (Connection conn = open()) {
            try (Statement stmt = conn.createStatement()) {
                dropIfExists(stmt, "jdbc_labels");
                stmt.executeUpdate("CREATE TABLE jdbc_labels (ID INTEGER, LABEL STRING)");
                stmt.executeUpdate("INSERT INTO jdbc_labels (ID, LABEL) VALUES (7, 'seven')");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT ID, LABEL FROM jdbc_labels")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt("ID"));
                assertEquals("seven", rs.getString("LABEL"));
                assertFalse(rs.next());
            }

            try (Statement stmt = conn.createStatement()) {
                dropIfExists(stmt, "jdbc_labels");
            }
        }
    }

    @Test
    void resultSetMetadataDescribesColumns() throws SQLException {
        try (Connection conn = open()) {
            try (Statement stmt = conn.createStatement()) {
                dropIfExists(stmt, "jdbc_meta");
                stmt.executeUpdate("CREATE TABLE jdbc_meta (ID INTEGER, NAME STRING)");
                stmt.executeUpdate("INSERT INTO jdbc_meta (ID, NAME) VALUES (1, 'one')");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT ID, NAME FROM jdbc_meta")) {
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(2, meta.getColumnCount());
                assertEquals("ID", meta.getColumnName(1).toUpperCase());
                assertEquals("NAME", meta.getColumnName(2).toUpperCase());
            }

            try (Statement stmt = conn.createStatement()) {
                dropIfExists(stmt, "jdbc_meta");
            }
        }
    }

    @Test
    void databaseMetaDataAnswersHibernateStyleQuestions() throws SQLException {
        try (Connection conn = open()) {
            DatabaseMetaData meta = conn.getMetaData();
            assertEquals("DieselDB", meta.getDatabaseProductName());
            assertEquals("DieselDB JDBC Driver", meta.getDriverName());
            assertEquals(0, meta.getDriverMajorVersion());
            assertEquals(1, meta.getDriverMinorVersion());
            assertTrue(meta.supportsTransactions());
            assertFalse(meta.supportsBatchUpdates());
            assertEquals("\"", meta.getIdentifierQuoteString());

            // Hibernate probes catalogs/schemas at bootstrap; must not throw.
            try (ResultSet rs = meta.getCatalogs()) {
                while (rs.next()) {
                    assertNotNull(rs.getString(1));
                }
            }
            try (ResultSet rs = meta.getSchemas()) {
                while (rs.next()) {
                    assertNotNull(rs.getString(1));
                }
            }
            try (ResultSet rs = meta.getTableTypes()) {
                while (rs.next()) {
                    assertNotNull(rs.getString(1));
                }
            }
        }
    }

    @Test
    void sqlErrorSurfacesAsSQLException() throws SQLException {
        try (Connection conn = open();
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.executeQuery("SELECT * FROM table_that_does_not_exist"));
            assertNotNull(ex.getMessage());
            assertFalse(ex.getMessage().isEmpty());
        }
    }

    @Test
    void closedConnectionRejectsStatements() throws SQLException {
        Connection conn = open();
        conn.close();
        assertTrue(conn.isClosed());
        assertThrows(SQLException.class, conn::createStatement);
    }

    @Test
    void foreignUrlIsRejectedByParse() {
        assertThrows(SQLException.class, () -> DieselDriver.parseUrl("jdbc:mysql://h/db"));
    }
}

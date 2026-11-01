package com.diesel.jdbc;

import diesel.LibpqTestServer;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Prompt 55 acceptance: simple entity CRUD through Hibernate 6 over the
 * DieselDB JDBC driver.
 *
 * <p>Configuration notes: assigned IDs (no generation strategies, so Hibernate
 * never needs RETURNING/getGeneratedKeys), hbm2ddl disabled (DieselDB owns the
 * DDL dialect), PostgreSQL dialect for SQL shaping (the wire protocol is
 * libpq-compatible).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HibernateTest {

    @TempDir
    static Path tempDir;

    private LibpqTestServer server;
    private SessionFactory sessionFactory;

    @BeforeAll
    void setUp() throws Exception {
        server = LibpqTestServer.start(tempDir);

        // Create the table in DieselDB's own dialect before Hibernate touches it.
        Properties props = new Properties();
        props.setProperty("user", "testuser");
        props.setProperty("password", "testpass");
        try (Connection conn = DriverManager.getConnection(server.jdbcUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE HIB_USERS (ID INTEGER, NAME STRING)");
        }

        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        cfg.setProperty("hibernate.connection.driver_class", "com.diesel.jdbc.DieselDriver");
        cfg.setProperty("hibernate.connection.url", server.jdbcUrl());
        cfg.setProperty("hibernate.connection.user", "testuser");
        cfg.setProperty("hibernate.connection.password", "testpass");
        cfg.setProperty("hibernate.hbm2ddl.auto", "none");
        cfg.setProperty("hibernate.show_sql", "true");
        cfg.setProperty("hibernate.format_sql", "false");
        cfg.setProperty("hibernate.order_inserts", "false");
        cfg.addAnnotatedClass(User.class);
        sessionFactory = cfg.buildSessionFactory();
    }

    @AfterAll
    void tearDown() {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void entityCrudLifecycle() {
        // CREATE
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            User user = new User();
            user.setId(1);
            user.setName("Alice");
            session.persist(user);
            tx.commit();
        }

        // READ
        try (Session session = sessionFactory.openSession()) {
            User loaded = session.get(User.class, 1);
            assertNotNull(loaded, "persisted entity must be readable");
            assertEquals("Alice", loaded.getName());
        }

        // UPDATE
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            User user = session.get(User.class, 1);
            user.setName("Alice Cooper");
            session.merge(user);
            tx.commit();
        }

        // verify UPDATE
        try (Session session = sessionFactory.openSession()) {
            User loaded = session.get(User.class, 1);
            assertNotNull(loaded);
            assertEquals("Alice Cooper", loaded.getName());
        }

        // DELETE
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            User user = session.get(User.class, 1);
            session.delete(user);
            tx.commit();
        }

        // verify DELETE
        try (Session session = sessionFactory.openSession()) {
            assertNull(session.get(User.class, 1), "deleted entity must not be readable");
        }
    }

    @Test
    void hqlQueryOverEntity() {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            User u1 = new User();
            u1.setId(10);
            u1.setName("Bob");
            User u2 = new User();
            u2.setId(11);
            u2.setName("Carol");
            session.persist(u1);
            session.persist(u2);
            tx.commit();
        }

        try (Session session = sessionFactory.openSession()) {
            // Resolve the registered entity name explicitly: for a nested
            // static class Hibernate may register more than the simple name.
            String entityName = sessionFactory.getMetamodel()
                    .entity(User.class).getName();
            List<User> found = session
                    .createQuery("from " + entityName + " where name = :n", User.class)
                    .setParameter("n", "Bob")
                    .list();
            assertEquals(1, found.size());
            assertEquals(10, found.get(0).getId());

            List<User> all = session.createQuery("from " + entityName, User.class).list();
            assertEquals(2, all.size());
        }

        // cleanup
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            session.createQuery("delete from " + sessionFactory.getMetamodel()
                    .entity(User.class).getName()).executeUpdate();
            tx.commit();
        }
    }

    @Entity
    @Table(name = "HIB_USERS")
    public static class User {
        @Id
        @Column(name = "ID")
        private Integer id;

        @Column(name = "NAME")
        private String name;

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}

package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NullSafetyQueryTest extends AbstractDieselTest {

    /* ===== null-logic basics ===== */

    @Test @Order(1)
    void whereFlagTrue() {
        dropTable("NULL_TEST");
        runExec("TrueFalseNullTest", "create table",
                "CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER)");
        runExec("TrueFalseNullTest", "insert flag true",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25)");
        runExec("TrueFalseNullTest", "insert flag false",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30)");
        runExec("TrueFalseNullTest", "insert null in insert",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL)");
        runSelectCount("TrueFalseNullTest", "where flag = true",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE", 1);
    }

    @Test @Order(2)
    void whereFlagFalse() {
        runSelectCount("TrueFalseNullTest", "where flag = false",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE", 1);
    }

    @Test @Order(3)
    void whereColIsNull() {
        runSelectCount("TrueFalseNullTest", "where col is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL", 1);
    }

    @Test @Order(4)
    void whereColIsNotNull() {
        runSelectCount("TrueFalseNullTest", "where col is not null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL", 2);
    }

    @Test @Order(5)
    void whereAgeIsNull() {
        runSelectCount("TrueFalseNullTest", "where age is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL", 1);
    }

    /* ===== UPDATE set null ===== */

    @Test @Order(6)
    void updateSetNull() {
        runExec("TrueFalseNullTest", "update set null in update",
                "UPDATE NULL_TEST SET COL = NULL WHERE ID = 1");
        runSelectCount("TrueFalseNullTest", "where col is null after update",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL", 2);
    }

    /* ===== null equality returns empty ===== */

    @Test @Order(7)
    void whereColEqualNullReturnsEmpty() {
        runSelectCount("TrueFalseNullTest", "where col = null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL", 0);
    }

    @Test @Order(8)
    void whereColNotEqualNullReturnsEmpty() {
        runSelectCount("TrueFalseNullTest", "where col != null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL", 0);
    }

    @Test @Order(9)
    void prompt57SelectStarColEqualNull() {
        runSelectCount("TrueFalseNullTest", "prompt 57 select * where col = null returns empty",
                "SELECT * FROM NULL_TEST WHERE COL = NULL", 0);
    }

    @Test @Order(10)
    void prompt57SelectStarColNotEqualNull() {
        runSelectCount("TrueFalseNullTest", "prompt 57 select * where col != null returns empty",
                "SELECT * FROM NULL_TEST WHERE COL != NULL", 0);
    }

    @Test @Order(11)
    void prompt58SelectStarColIsNull() {
        runSelectCount("TrueFalseNullTest", "prompt 58 select * where col is null returns rows with null col",
                "SELECT * FROM NULL_TEST WHERE COL IS NULL", 2);
    }

    @Test @Order(12)
    void prompt59OrWithNull() {
        runSelectCount("TrueFalseNullTest", "prompt 59 select * where col = 25 or col is null returns value and null rows",
                "SELECT * FROM NULL_TEST WHERE AGE = 25 OR AGE IS NULL", 2);
    }

    @Test @Order(13)
    void prompt59AndNotNull() {
        runSelectCount("TrueFalseNullTest", "prompt 59 select * where col = 25 and col is not null returns only value row",
                "SELECT * FROM NULL_TEST WHERE AGE = 25 AND AGE IS NOT NULL", 1);
    }

    /* ===== comparison operators with null ===== */

    @Test @Order(14)
    void whereAgeLessThanNull() {
        runSelectCount("TrueFalseNullTest", "where age < null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL", 0);
    }

    @Test @Order(15)
    void whereAgeGreaterThanNull() {
        runSelectCount("TrueFalseNullTest", "where age > null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL", 0);
    }

    @Test @Order(16)
    void whereAgeLessEqualNull() {
        runSelectCount("TrueFalseNullTest", "where age <= null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL", 0);
    }

    @Test @Order(17)
    void whereAgeGreaterEqualNull() {
        runSelectCount("TrueFalseNullTest", "where age >= null returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL", 0);
    }

    @Test @Order(18)
    void whereColNotEqualExcludesNull() {
        runSelectCount("TrueFalseNullTest", "where col != 'A' excludes null rows",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A'", 1);
    }

    @Test @Order(19)
    void whereAgeLessThanExcludesNull() {
        runSelectCount("TrueFalseNullTest", "where age < 30 excludes null row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30", 1);
    }

    @Test @Order(20)
    void whereAgeOrColEqualNullKeepsMatching() {
        runSelectCount("TrueFalseNullTest", "where age = 25 or col = null keeps only matching row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL", 1);
    }

    @Test @Order(21)
    void whereColEqualNullAndAgeReturnsEmpty() {
        runSelectCount("TrueFalseNullTest", "where col = null and age = 25 returns empty",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25", 0);
    }

    /* ===== three-valued logic AND / OR / NOT ===== */

    @Test @Order(22)
    void whereTrueAndUnknown() {
        runSelectCount("TrueFalseNullTest", "where true and unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL", 0);
    }

    @Test @Order(23)
    void whereFalseAndUnknown() {
        runSelectCount("TrueFalseNullTest", "where false and unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL", 0);
    }

    @Test @Order(24)
    void whereUnknownAndUnknown() {
        runSelectCount("TrueFalseNullTest", "where unknown and unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL", 0);
    }

    @Test @Order(25)
    void whereNotTrueAndUnknown() {
        runSelectCount("TrueFalseNullTest", "where not true and unknown keeps only false row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL)", 1);
    }

    @Test @Order(26)
    void whereTrueOrUnknown() {
        runSelectCount("TrueFalseNullTest", "where true or unknown includes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL", 1);
    }

    @Test @Order(27)
    void whereFalseOrUnknown() {
        runSelectCount("TrueFalseNullTest", "where false or unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL", 0);
    }

    @Test @Order(28)
    void whereUnknownOrUnknown() {
        runSelectCount("TrueFalseNullTest", "where unknown or unknown excludes row",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL", 0);
    }

    @Test @Order(29)
    void whereFalseOrTrueOrUnknownOrTrue() {
        runSelectCount("TrueFalseNullTest", "where false or true and unknown or true include rows",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL", 2);
    }

    @Test @Order(30)
    void whereNotTrueOrUnknown() {
        runSelectCount("TrueFalseNullTest", "where not true or unknown excludes all rows",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL)", 0);
    }

    /* ===== UPDATE / DELETE with null predicates ===== */

    @Test @Order(31)
    void updateWhereColIsNull() {
        runExec("TrueFalseNullTest", "update where col is null",
                "UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL");
        runSelectCount("TrueFalseNullTest", "select after update where col is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40", 2);
    }

    @Test @Order(32)
    void updateWhereColIsNotNull() {
        runExec("TrueFalseNullTest", "update where col is not null",
                "UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL");
        runSelectCount("TrueFalseNullTest", "select after update where col is not null",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50", 1);
    }

    @Test @Order(33)
    void deleteWhereColIsNull() {
        runExec("TrueFalseNullTest", "delete where col is null",
                "DELETE FROM NULL_TEST WHERE COL IS NULL");
        runSelectCount("TrueFalseNullTest", "select after delete where col is null",
                "SELECT ID, FLAG, COL FROM NULL_TEST", 1);
    }

    @Test @Order(34)
    void deleteWhereColIsNotNull() {
        runExec("TrueFalseNullTest", "delete where col is not null",
                "DELETE FROM NULL_TEST WHERE COL IS NOT NULL");
        runSelectCount("TrueFalseNullTest", "select after delete where col is not null",
                "SELECT ID, FLAG, COL FROM NULL_TEST", 0);
    }

    /* ===== OR/unknown with re-inserted data ===== */

    @Test @Order(35)
    void orUnknownAfterReinsert() {
        runExec("TrueFalseNullTest", "reinsert row a for or logic",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25)");
        runExec("TrueFalseNullTest", "reinsert row b for or logic",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30)");
        runExec("TrueFalseNullTest", "reinsert null row for or logic",
                "INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL)");
        runExec("TrueFalseNullTest", "update where false or unknown or true",
                "UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL");
        runSelectCount("TrueFalseNullTest", "select after update with or unknown",
                "SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77", 1);
    }

    @Test @Order(36)
    void deleteWhereTrueOrUnknown() {
        runExec("TrueFalseNullTest", "delete where true or unknown",
                "DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL");
        runSelectCount("TrueFalseNullTest", "select after delete with or unknown",
                "SELECT ID, FLAG, COL FROM NULL_TEST", 2);
    }

    /* ===== aggregate functions with NULLs ===== */

    @Test @Order(37)
    void aggregateWithNulls() {
        dropTable("AGG_TEST");
        runExec("TrueFalseNullTest", "prompt 60 create agg table",
                "CREATE TABLE AGG_TEST (ID LONG PRIMARY KEY SEQUENCE(agg_test_seq 1 1), AMOUNT INTEGER)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount 10",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (10)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount 20",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (20)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount null",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount 30",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (30)");
        runExec("TrueFalseNullTest", "prompt 60 insert amount null 2",
                "INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL)");
        runSelectCount("TrueFalseNullTest", "prompt 60 select * returns all rows incl nulls",
                "SELECT * FROM AGG_TEST", 5);
        checkAggregate("TrueFalseNullTest", "prompt 60 count star counts all rows",
                "SELECT COUNT(*) FROM AGG_TEST", "COUNT(*)", 5L);
        checkAggregate("TrueFalseNullTest", "prompt 60 count column skips null",
                "SELECT COUNT(AMOUNT) FROM AGG_TEST", "COUNT(AMOUNT)", 3L);
        checkAggregate("TrueFalseNullTest", "prompt 60 sum skips null",
                "SELECT SUM(AMOUNT) FROM AGG_TEST", "SUM(AMOUNT)", 60);
        checkAggregate("TrueFalseNullTest", "prompt 60 avg skips null",
                "SELECT AVG(AMOUNT) FROM AGG_TEST", "AVG(AMOUNT)", 20);
        checkAggregate("TrueFalseNullTest", "prompt 60 min skips null",
                "SELECT MIN(AMOUNT) FROM AGG_TEST", "MIN(AMOUNT)", 10);
        checkAggregate("TrueFalseNullTest", "prompt 60 max skips null",
                "SELECT MAX(AMOUNT) FROM AGG_TEST", "MAX(AMOUNT)", 30);
        dropTable("AGG_TEST");
    }

    /* ===== case sensitivity ===== */

    @Test @Order(38)
    void caseSensitivityBasic() {
        dropTable("CASE_TEST");
        dropTable("MyTable");
        runExec("CaseSensitivityTest", "create table",
                "CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING)");
        runExec("CaseSensitivityTest", "insert john",
                "INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value')");
        runSelectCount("CaseSensitivityTest", "where name = 'John' finds row",
                "SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John'", 1);
        runSelectCount("CaseSensitivityTest", "where name = 'JOHN' returns no rows",
                "SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN'", 0);
        runSelectCount("CaseSensitivityTest", "quoted column identifier myColumn",
                "SELECT \"myColumn\" FROM CASE_TEST", 1);
        runExec("CaseSensitivityTest", "create quoted table",
                "CREATE TABLE \"MyTable\" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING)");
        runExec("CaseSensitivityTest", "insert into quoted table",
                "INSERT INTO \"MyTable\" (NAME) VALUES ('test')");
        runSelectCount("CaseSensitivityTest", "select from quoted table",
                "SELECT * FROM \"MyTable\"", 1);
        dropTable("CASE_TEST");
        dropTable("MyTable");
    }

    /* ===== prompt 62-65: case sensitivity + boolean ===== */

    @Test @Order(39)
    void prompt62CaseSensitivity() {
        dropTable("P62_TEST");
        runExec("Prompt62Test", "prompt 62 create users table",
                "CREATE TABLE P62_TEST (ID INTEGER, NAME STRING)");
        runExec("Prompt62Test", "prompt 62 insert John",
                "INSERT INTO P62_TEST (ID, NAME) VALUES (1, 'John')");
        runExec("Prompt62Test", "prompt 62 insert jane",
                "INSERT INTO P62_TEST (ID, NAME) VALUES (2, 'jane')");
        runSelectCount("Prompt62Test", "prompt 62 where name = 'John' returns only the John row",
                "SELECT * FROM P62_TEST WHERE NAME = 'John'", 1);
        runSelectCount("Prompt62Test", "prompt 63 where name = 'JOHN' returns no rows",
                "SELECT * FROM P62_TEST WHERE NAME = 'JOHN'", 0);
        runSelectCount("Prompt62Test", "prompt 63 where name = 'John' returns the John row",
                "SELECT * FROM P62_TEST WHERE NAME = 'John'", 1);
        runExec("Prompt62Test", "prompt 64 insert null name",
                "INSERT INTO P62_TEST (ID, NAME) VALUES (3, NULL)");
        runSelectCount("Prompt62Test", "prompt 64 where name is null returns only the null name row",
                "SELECT * FROM P62_TEST WHERE NAME IS NULL", 1);
        runSelectCount("Prompt62Test", "prompt 64 where name = null returns no rows",
                "SELECT * FROM P62_TEST WHERE NAME = NULL", 0);
        dropTable("P62_TEST");
    }

    @Test @Order(40)
    void prompt65BooleanFiltering() {
        dropTable("BOOL_TEST");
        runExec("Prompt65Test", "prompt 65 create bool table",
                "CREATE TABLE BOOL_TEST (ID LONG PRIMARY KEY SEQUENCE(bool_test_seq 1 1), FLAG BOOLEAN)");
        runExec("Prompt65Test", "prompt 65 insert flag true",
                "INSERT INTO BOOL_TEST (FLAG) VALUES (TRUE)");
        runExec("Prompt65Test", "prompt 65 insert flag false",
                "INSERT INTO BOOL_TEST (FLAG) VALUES (FALSE)");
        runSelectCount("Prompt65Test", "prompt 65 where flag = true returns only the true row",
                "SELECT * FROM BOOL_TEST WHERE FLAG = TRUE", 1);
        runSelectCount("Prompt65Test", "prompt 65 where flag = false returns only the false row",
                "SELECT * FROM BOOL_TEST WHERE FLAG = FALSE", 1);
        dropTable("BOOL_TEST");
    }

    /* ===== prompt 66-67: basic transaction BEGIN/ROLLBACK/COMMIT ===== */

    @Test @Order(41)
    void prompt66BasicTransaction() {
        dropTable("TXN66_TEST");
        runExec("Prompt66Test", "prompt 66 create transaction table",
                "CREATE TABLE TXN66_TEST (ID LONG PRIMARY KEY SEQUENCE(txn66_seq 1 1), NAME STRING)");
        check(database.isAutoCommit(), "Prompt66Test / autoCommit is true by default");
        runExec("Prompt66Test", "prompt 66 insert without begin auto-commits",
                "INSERT INTO TXN66_TEST (NAME) VALUES ('prompt66-auto')");
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-auto'") == 1,
                "Prompt66Test / INSERT without BEGIN is committed and visible via SELECT");
        String beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        java.util.UUID prompt66TxId = java.util.UUID.fromString(beginResult.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN66_TEST (NAME) VALUES ('prompt66-rolled')", prompt66TxId);
        Object inTx66 = database.executeQuery("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-rolled'", prompt66TxId);
        check(inTx66 instanceof java.util.List && ((java.util.List<?>) inTx66).size() == 1,
                "Prompt66Test / BEGIN+INSERT row is visible inside the current transaction");
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-rolled'") == 0,
                "Prompt66Test / BEGIN+INSERT row is not visible outside the transaction");
        database.executeQuery("ROLLBACK", prompt66TxId);
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-rolled'") == 0,
                "Prompt66Test / BEGIN+INSERT row is not inserted after ROLLBACK");
        check(countRows("SELECT * FROM TXN66_TEST WHERE NAME = 'prompt66-auto'") == 1,
                "Prompt66Test / auto-committed row is still present after ROLLBACK");
        database.setAutoCommit(true);
        dropTable("TXN66_TEST");
    }

    @Test @Order(42)
    void prompt67MultiInsertInTransaction() {
        dropTable("TXN67_TEST");
        runExec("Prompt67Test", "prompt 67 create transaction table",
                "CREATE TABLE TXN67_TEST (ID LONG PRIMARY KEY SEQUENCE(txn67_seq 1 1), NAME STRING)");
        check(database.isAutoCommit(), "Prompt67Test / autoCommit is true by default");
        String beginResult = (String) database.executeQuery("BEGIN TRANSACTION", null);
        java.util.UUID prompt67TxId = java.util.UUID.fromString(beginResult.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN67_TEST (NAME) VALUES ('prompt67-first')", prompt67TxId);
        Object firstInTx = database.executeQuery("SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first'", prompt67TxId);
        check(firstInTx instanceof java.util.List && ((java.util.List<?>) firstInTx).size() == 1,
                "Prompt67Test / SELECT right after the first INSERT sees the row inside the transaction");
        database.executeQuery("INSERT INTO TXN67_TEST (NAME) VALUES ('prompt67-second')", prompt67TxId);
        Object allInTx = database.executeQuery("SELECT * FROM TXN67_TEST", prompt67TxId);
        check(allInTx instanceof java.util.List && ((java.util.List<?>) allInTx).size() == 2,
                "Prompt67Test / SELECT after the second INSERT sees both rows inserted in the same transaction");
        check(!database.isAutoCommit(), "Prompt67Test / autoCommit is false while the transaction is open");
        runSelectCount("Prompt67Test", "prompt 67 select before commit is isolated from the transaction",
                "SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first'", 0);
        database.executeQuery("COMMIT", prompt67TxId);
        runSelectCount("Prompt67Test", "prompt 67 both rows visible only after COMMIT",
                "SELECT * FROM TXN67_TEST", 2);
        check(countRows("SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first'") == 1,
                "Prompt67Test / first row is visible only after COMMIT");
        check(countRows("SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-second'") == 1,
                "Prompt67Test / second row is visible only after COMMIT");
        check(!database.isInTransaction(prompt67TxId),
                "Prompt67Test / COMMIT ends the transaction");
        database.setAutoCommit(true);
        dropTable("TXN67_TEST");
    }

    /* ===== prompt 68: multi-client transactions + concurrent ===== */

    @Test @Order(43)
    void prompt68MultiClient() {
        dropTable("TXN68_TEST");
        runExec("Prompt68Test", "prompt 68 create multi-client table",
                "CREATE TABLE TXN68_TEST (ID LONG PRIMARY KEY SEQUENCE(txn68_seq 1 1), CLIENT STRING, NAME STRING)");
        String beginA = (String) database.executeQuery("BEGIN TRANSACTION", null);
        java.util.UUID clientA = java.util.UUID.fromString(beginA.split(": ")[1]);
        String beginB = (String) database.executeQuery("BEGIN TRANSACTION", null);
        java.util.UUID clientB = java.util.UUID.fromString(beginB.split(": ")[1]);
        check(!clientA.equals(clientB) && database.isInTransaction(clientA) && database.isInTransaction(clientB),
                "Prompt68Test / two clients hold two distinct active transaction sessions");
        database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('clientA', 'prompt68-committed')", clientA);
        database.executeQuery("COMMIT", clientA);
        check(!database.isInTransaction(clientA), "Prompt68Test / client A's COMMIT ends its transaction");
        runSelectCount("Prompt68Test", "prompt 68 reader sees the other client's committed row",
                "SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-committed'", 1);
        String beginA2 = (String) database.executeQuery("BEGIN TRANSACTION", null);
        java.util.UUID clientA2 = java.util.UUID.fromString(beginA2.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('clientA', 'prompt68-uncommitted')", clientA2);
        runSelectCount("Prompt68Test", "prompt 68 reader does not see the other client's uncommitted row",
                "SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted'", 0);
        database.executeQuery("COMMIT", clientA2);
        runSelectCount("Prompt68Test", "prompt 68 reader sees the row after the writer's COMMIT",
                "SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted'", 1);
        Object bSnapshot = database.executeQuery("SELECT * FROM TXN68_TEST", clientB);
        check(bSnapshot instanceof java.util.List && ((java.util.List<?>) bSnapshot).size() == 0,
                "Prompt68Test / reader's own transaction keeps its BEGIN-time snapshot (other clients' commits not visible)");
        String beginA3 = (String) database.executeQuery("BEGIN TRANSACTION", null);
        java.util.UUID clientA3 = java.util.UUID.fromString(beginA3.split(": ")[1]);
        database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('clientA', 'prompt68-dirty')", clientA3);
        Object dirtyRead = database.executeQuery("SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-dirty'", clientB);
        check(dirtyRead instanceof java.util.List && ((java.util.List<?>) dirtyRead).size() == 1,
                "Prompt68Test / reader at READ_UNCOMMITTED isolation sees the writer's uncommitted row (dirty read)");
        database.executeQuery("ROLLBACK", clientA3);
        Object afterRollback = database.executeQuery("SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-dirty'", clientB);
        check(afterRollback instanceof java.util.List && ((java.util.List<?>) afterRollback).size() == 0,
                "Prompt68Test / reader no longer sees the row after the writer's ROLLBACK");

        runPrompt68ConcurrentClients();

        database.setAutoCommit(true);
        dropTable("TXN68_TEST");
    }

    private void runPrompt68ConcurrentClients() {
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        java.util.concurrent.CountDownLatch writerInserted = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch readerVerified = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch writerCommitted = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger uncommittedVisible = new java.util.concurrent.atomic.AtomicInteger(-1);
        java.util.concurrent.atomic.AtomicInteger committedVisible = new java.util.concurrent.atomic.AtomicInteger(-1);

        java.util.concurrent.Future<?> writerFuture = executor.submit(() -> {
            String begin = (String) database.executeQuery("BEGIN TRANSACTION", null);
            java.util.UUID writerTx = java.util.UUID.fromString(begin.split(": ")[1]);
            for (int i = 1; i <= 5; i++) {
                database.executeQuery("INSERT INTO TXN68_TEST (CLIENT, NAME) VALUES ('concurrent', 'prompt68-concurrent-" + i + "')", writerTx);
            }
            writerInserted.countDown();
            readerVerified.await();
            database.executeQuery("COMMIT", writerTx);
            writerCommitted.countDown();
            return null;
        });

        java.util.concurrent.Future<?> readerFuture = executor.submit(() -> {
            writerInserted.await();
            Object beforeCommit = database.executeQuery("SELECT * FROM TXN68_TEST WHERE CLIENT = 'concurrent'", null);
            uncommittedVisible.set(beforeCommit instanceof java.util.List ? ((java.util.List<?>) beforeCommit).size() : -1);
            readerVerified.countDown();
            writerCommitted.await();
            Object afterCommit = database.executeQuery("SELECT * FROM TXN68_TEST WHERE CLIENT = 'concurrent'", null);
            committedVisible.set(afterCommit instanceof java.util.List ? ((java.util.List<?>) afterCommit).size() : -1);
            return null;
        });

        try {
            writerFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
            readerFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Prompt68Test concurrent client test failed", e);
        } finally {
            executor.shutdownNow();
        }

        check(uncommittedVisible.get() == 0,
                "Prompt68Test / concurrent reader sees 0 of the writer's rows while the transaction is open");
        check(committedVisible.get() == 5,
                "Prompt68Test / concurrent reader sees all 5 writer rows only after COMMIT");
    }
}

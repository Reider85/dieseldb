# AllTestsSampleTest query timings

Generated: Sat Aug 15 11:54:30 MSK 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | AdvancedTest | simple select by primary key | OK | 51.93 | SELECT ID, NAME FROM USERS WHERE ID = 500 |
| 2 | AdvancedTest | simple select by name | OK | 7.33 | SELECT ID, NAME FROM USERS WHERE NAME = 'User500' |
| 3 | AdvancedTest | complex select with multi-column and conditions | OK | 19.05 | SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500') |
| 4 | AdvancedTest | complex select with or limit offset | OK | 10.99 | SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5 |
| 5 | AliasesTest | simple select with alias order by | OK | 9.52 | SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName |
| 6 | AliasesTest | simple select with as alias order by | OK | 10.49 | SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName |
| 7 | AliasesTest | complex select min max avg with join and group by | OK | 50.92 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC |
| 8 | AliasesTest | complex select with multiple inner joins | OK | 10.78 | SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5 |
| 9 | GroupByTest | simple group by min max avg | OK | 18.69 | SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME |
| 10 | GroupByTest | simple group by sum count | OK | 16.67 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME |
| 11 | GroupByTest | complex group by date having | OK | 18.00 | SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0 |
| 12 | GroupByTest | complex group by join string date | OK | 21.70 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC |
| 13 | InTest | simple in on btree index | OK | 2.53 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) |
| 14 | InTest | simple in on primary key | OK | 4.71 | SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) |
| 15 | InTest | complex in with and | OK | 5.62 | SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000 |
| 16 | InTest | complex in with or | OK | 11.84 | SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000 |
| 17 | JoinTest | simple inner join on primary key | OK | 12.05 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 18 | JoinTest | simple inner join on non indexed field | OK | 12.46 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00 |
| 19 | JoinTest | complex full join on primary key | OK | 14.72 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 20 | JoinTest | complex inner join with and or in on | OK | 21.24 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502) |
| 21 | LikeTest | simple like on name | OK | 7.60 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' |
| 22 | LikeTest | simple like on user code | OK | 3.89 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' |
| 23 | LikeTest | complex like with and | OK | 4.37 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000 |
| 24 | LikeTest | complex like with or | OK | 6.40 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000 |
| 25 | OrderByTest | simple order by name | OK | 3.24 | SELECT ID, NAME FROM USERS ORDER BY NAME |
| 26 | OrderByTest | simple order by age desc | OK | 3.27 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC |
| 27 | OrderByTest | complex join order by primary key | OK | 5594.93 | SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.USER_ID > 0 OR PROFILES.USER_ID IS NOT NULL ORDER BY USERS.ID |
| 28 | OrderByTest | complex join order by non indexed | OK | 4051.07 | SELECT USERS.ID, USERS.BALANCE, PROFILES.NON_INDEXED FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.NON_INDEXED LIKE 'Non%' OR PROFILES.NON_INDEXED IS NOT NULL ORDER BY USERS.BALANCE |
| 29 | PerformanceTest | simple select where age | OK | 3.73 | SELECT NAME, AGE FROM USERS WHERE AGE < 30 |
| 30 | PerformanceTest | simple select clustered index | OK | 1.26 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' |
| 31 | PerformanceTest | complex select age and active | OK | 1.93 | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE |
| 32 | PerformanceTest | complex select parenthesized or | OK | 7.32 | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500 |
| 33 | PersistenceTest | create table | OK | 1.95 | CREATE TABLE PERSIST_TEST (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID) |
| 34 | PersistenceTest | insert alice | OK | 5.15 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000') |
| 35 | PersistenceTest | insert bob full schema | OK | 4.98 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000') |
| 36 | PersistenceTest | select from persisted table | OK | 3.63 | SELECT NAME, AGE FROM PERSIST_TEST WHERE AGE = 25 |
| 37 | SubqueriesTest | simple subquery in in clause | OK | 48.55 | SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10 |
| 38 | SubqueriesTest | simple subquery in where | OK | 18.91 | SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 39 | SubqueriesTest | complex subquery in column where group by having | OK | 600.77 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10 |
| 40 | SubqueriesTest | complex subquery in column inner join on | OK | 15.79 | SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 41 | TrueFalseNullTest | create table | OK | 0.65 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 42 | TrueFalseNullTest | insert flag true | OK | 1.64 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 43 | TrueFalseNullTest | insert flag false | OK | 1.80 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 44 | TrueFalseNullTest | insert null in insert | OK | 1.84 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 45 | TrueFalseNullTest | where flag = true | OK | 1.32 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 46 | TrueFalseNullTest | where flag = false | OK | 3.44 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 47 | TrueFalseNullTest | where col is null | OK | 2.50 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 48 | TrueFalseNullTest | where col is not null | OK | 1.19 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 49 | TrueFalseNullTest | where age is null | OK | 0.69 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 50 | TrueFalseNullTest | update set null in update | OK | 3.17 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 51 | TrueFalseNullTest | where col is null after update | OK | 0.82 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 52 | TrueFalseNullTest | where col = null returns empty | OK | 0.73 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 53 | TrueFalseNullTest | where col != null returns empty | OK | 0.70 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL |
| 54 | TrueFalseNullTest | prompt 57 select * where col = null returns empty | OK | 0.94 | SELECT * FROM NULL_TEST WHERE COL = NULL |
| 55 | TrueFalseNullTest | prompt 57 select * where col != null returns empty | OK | 0.68 | SELECT * FROM NULL_TEST WHERE COL != NULL |
| 56 | TrueFalseNullTest | prompt 58 select * where col is null returns rows with null col | OK | 0.72 | SELECT * FROM NULL_TEST WHERE COL IS NULL |
| 57 | TrueFalseNullTest | prompt 59 select * where col = 25 or col is null returns value and null rows | OK | 2.90 | SELECT * FROM NULL_TEST WHERE AGE = 25 OR AGE IS NULL |
| 58 | TrueFalseNullTest | prompt 59 select * where col = 25 and col is not null returns only value row | OK | 1.28 | SELECT * FROM NULL_TEST WHERE AGE = 25 AND AGE IS NOT NULL |
| 59 | TrueFalseNullTest | where age < null returns empty | OK | 1.29 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL |
| 60 | TrueFalseNullTest | where age > null returns empty | OK | 1.31 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL |
| 61 | TrueFalseNullTest | where age <= null returns empty | OK | 0.88 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL |
| 62 | TrueFalseNullTest | where age >= null returns empty | OK | 0.79 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL |
| 63 | TrueFalseNullTest | where col != 'A' excludes null rows | OK | 1.26 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A' |
| 64 | TrueFalseNullTest | where age < 30 excludes null row | OK | 0.72 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30 |
| 65 | TrueFalseNullTest | where age = 25 or col = null keeps only matching row | OK | 1.02 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 66 | TrueFalseNullTest | where col = null and age = 25 returns empty | OK | 0.88 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25 |
| 67 | TrueFalseNullTest | where true and unknown excludes row | OK | 0.91 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL |
| 68 | TrueFalseNullTest | where false and unknown excludes row | OK | 0.85 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL |
| 69 | TrueFalseNullTest | where unknown and unknown excludes row | OK | 0.95 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL |
| 70 | TrueFalseNullTest | where not true and unknown keeps only false row | OK | 1.80 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL) |
| 71 | TrueFalseNullTest | where true or unknown includes row | OK | 6.28 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 72 | TrueFalseNullTest | where false or unknown excludes row | OK | 1.05 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL |
| 73 | TrueFalseNullTest | where unknown or unknown excludes row | OK | 0.96 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL |
| 74 | TrueFalseNullTest | where false or true and unknown or true include rows | OK | 0.89 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL |
| 75 | TrueFalseNullTest | where not true or unknown excludes all rows | OK | 1.20 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL) |
| 76 | TrueFalseNullTest | update where col is null | OK | 3.11 | UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL |
| 77 | TrueFalseNullTest | select after update where col is null | OK | 1.16 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40 |
| 78 | TrueFalseNullTest | update where col is not null | OK | 6.28 | UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL |
| 79 | TrueFalseNullTest | select after update where col is not null | OK | 1.69 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50 |
| 80 | TrueFalseNullTest | delete where col is null | OK | 3.41 | DELETE FROM NULL_TEST WHERE COL IS NULL |
| 81 | TrueFalseNullTest | select after delete where col is null | OK | 0.51 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 82 | TrueFalseNullTest | delete where col is not null | OK | 1.86 | DELETE FROM NULL_TEST WHERE COL IS NOT NULL |
| 83 | TrueFalseNullTest | select after delete where col is not null | OK | 0.60 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 84 | TrueFalseNullTest | reinsert row a for or logic | OK | 1.54 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 85 | TrueFalseNullTest | reinsert row b for or logic | OK | 1.52 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 86 | TrueFalseNullTest | reinsert null row for or logic | OK | 3.20 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 87 | TrueFalseNullTest | update where false or unknown or true | OK | 2.76 | UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL |
| 88 | TrueFalseNullTest | select after update with or unknown | OK | 1.12 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77 |
| 89 | TrueFalseNullTest | delete where true or unknown | OK | 2.14 | DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 90 | TrueFalseNullTest | select after delete with or unknown | OK | 0.63 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 91 | TrueFalseNullTest | prompt 60 create agg table | OK | 0.48 | CREATE TABLE AGG_TEST (ID LONG PRIMARY KEY SEQUENCE(agg_test_seq 1 1), AMOUNT INTEGER) |
| 92 | TrueFalseNullTest | prompt 60 insert amount 10 | OK | 1.75 | INSERT INTO AGG_TEST (AMOUNT) VALUES (10) |
| 93 | TrueFalseNullTest | prompt 60 insert amount 20 | OK | 1.68 | INSERT INTO AGG_TEST (AMOUNT) VALUES (20) |
| 94 | TrueFalseNullTest | prompt 60 insert amount null | OK | 1.54 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 95 | TrueFalseNullTest | prompt 60 insert amount 30 | OK | 3.64 | INSERT INTO AGG_TEST (AMOUNT) VALUES (30) |
| 96 | TrueFalseNullTest | prompt 60 insert amount null 2 | OK | 2.29 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 97 | TrueFalseNullTest | prompt 60 select * returns all rows incl nulls | OK | 0.88 | SELECT * FROM AGG_TEST |
| 98 | TrueFalseNullTest | prompt 60 count star counts all rows | OK | 0.84 | SELECT COUNT(*) FROM AGG_TEST |
| 99 | TrueFalseNullTest | prompt 60 count column skips null | OK | 0.63 | SELECT COUNT(AMOUNT) FROM AGG_TEST |
| 100 | TrueFalseNullTest | prompt 60 sum skips null | OK | 0.65 | SELECT SUM(AMOUNT) FROM AGG_TEST |
| 101 | TrueFalseNullTest | prompt 60 avg skips null | OK | 0.59 | SELECT AVG(AMOUNT) FROM AGG_TEST |
| 102 | TrueFalseNullTest | prompt 60 min skips null | OK | 1.02 | SELECT MIN(AMOUNT) FROM AGG_TEST |
| 103 | TrueFalseNullTest | prompt 60 max skips null | OK | 0.62 | SELECT MAX(AMOUNT) FROM AGG_TEST |
| 104 | CaseSensitivityTest | create table | OK | 0.75 | CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING) |
| 105 | CaseSensitivityTest | insert john | OK | 1.57 | INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value') |
| 106 | CaseSensitivityTest | where name = 'John' finds row | OK | 1.13 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John' |
| 107 | CaseSensitivityTest | where name = 'JOHN' returns no rows | OK | 3.36 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN' |
| 108 | CaseSensitivityTest | quoted column identifier myColumn | OK | 0.74 | SELECT "myColumn" FROM CASE_TEST |
| 109 | CaseSensitivityTest | create quoted table | OK | 0.48 | CREATE TABLE "MyTable" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING) |
| 110 | CaseSensitivityTest | insert into quoted table | OK | 2.01 | INSERT INTO "MyTable" (NAME) VALUES ('test') |
| 111 | CaseSensitivityTest | select from quoted table | OK | 0.73 | SELECT * FROM "MyTable" |
| 112 | TransactionTest | create table | OK | 0.47 | CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING) |
| 113 | TransactionTest | insert without begin auto-commits | OK | 1.43 | INSERT INTO TXN_TEST (NAME) VALUES ('auto48') |
| 114 | TransactionTest | set autocommit off | OK | 0.40 | SET AUTOCOMMIT = OFF |
| 115 | TransactionTest | set session autocommit on | OK | 0.15 | SET SESSION AUTOCOMMIT = ON |
| 116 | TransactionTest | set session autocommit off | OK | 0.09 | SET SESSION AUTOCOMMIT = OFF |
| 117 | TransactionTest | set autocommit on | OK | 0.09 | SET AUTOCOMMIT = ON |
| 118 | Prompt62Test | prompt 62 create users table | OK | 0.54 | CREATE TABLE USERS (ID INTEGER, NAME STRING) |
| 119 | Prompt62Test | prompt 62 insert John | OK | 1.81 | INSERT INTO USERS (ID, NAME) VALUES (1, 'John') |
| 120 | Prompt62Test | prompt 62 insert jane | OK | 1.78 | INSERT INTO USERS (ID, NAME) VALUES (2, 'jane') |
| 121 | Prompt62Test | prompt 62 where name = 'John' returns only the John row | OK | 0.92 | SELECT * FROM USERS WHERE NAME = 'John' |
| 122 | Prompt62Test | prompt 63 where name = 'JOHN' returns no rows | OK | 1.67 | SELECT * FROM USERS WHERE NAME = 'JOHN' |
| 123 | Prompt62Test | prompt 63 where name = 'John' returns the John row | OK | 1.50 | SELECT * FROM USERS WHERE NAME = 'John' |
| 124 | Prompt62Test | prompt 64 insert null name | OK | 2.36 | INSERT INTO USERS (ID, NAME) VALUES (3, NULL) |
| 125 | Prompt62Test | prompt 64 where name is null returns only the null name row | OK | 0.95 | SELECT * FROM USERS WHERE NAME IS NULL |
| 126 | Prompt62Test | prompt 64 where name = null returns no rows | OK | 0.78 | SELECT * FROM USERS WHERE NAME = NULL |
| 127 | Prompt65Test | prompt 65 create bool table | OK | 0.43 | CREATE TABLE BOOL_TEST (ID LONG PRIMARY KEY SEQUENCE(bool_test_seq 1 1), FLAG BOOLEAN) |
| 128 | Prompt65Test | prompt 65 insert flag true | OK | 1.38 | INSERT INTO BOOL_TEST (FLAG) VALUES (TRUE) |
| 129 | Prompt65Test | prompt 65 insert flag false | OK | 1.45 | INSERT INTO BOOL_TEST (FLAG) VALUES (FALSE) |
| 130 | Prompt65Test | prompt 65 where flag = true returns only the true row | OK | 1.00 | SELECT * FROM BOOL_TEST WHERE FLAG = TRUE |
| 131 | Prompt65Test | prompt 65 where flag = false returns only the false row | OK | 0.80 | SELECT * FROM BOOL_TEST WHERE FLAG = FALSE |
| 132 | Prompt66Test | prompt 66 create transaction table | OK | 0.46 | CREATE TABLE TXN66_TEST (ID LONG PRIMARY KEY SEQUENCE(txn66_seq 1 1), NAME STRING) |
| 133 | Prompt66Test | prompt 66 insert without begin auto-commits | OK | 1.50 | INSERT INTO TXN66_TEST (NAME) VALUES ('prompt66-auto') |
| 134 | Prompt67Test | prompt 67 create transaction table | OK | 0.48 | CREATE TABLE TXN67_TEST (ID LONG PRIMARY KEY SEQUENCE(txn67_seq 1 1), NAME STRING) |
| 135 | Prompt67Test | prompt 67 select before commit is isolated from the transaction | OK | 1.36 | SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first' |
| 136 | Prompt67Test | prompt 67 both rows visible only after COMMIT | OK | 0.64 | SELECT * FROM TXN67_TEST |
| 137 | Prompt68Test | prompt 68 create multi-client table | OK | 1.01 | CREATE TABLE TXN68_TEST (ID LONG PRIMARY KEY SEQUENCE(txn68_seq 1 1), CLIENT STRING, NAME STRING) |
| 138 | Prompt68Test | prompt 68 reader sees the other client's committed row | OK | 1.20 | SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-committed' |
| 139 | Prompt68Test | prompt 68 reader does not see the other client's uncommitted row | OK | 0.86 | SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted' |
| 140 | Prompt68Test | prompt 68 reader sees the row after the writer's COMMIT | OK | 0.99 | SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted' |

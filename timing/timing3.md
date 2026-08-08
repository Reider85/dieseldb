# AllTestsSampleTest query timings

Generated: Fri Aug 07 11:24:02 GMT+04:00 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | AdvancedTest | simple select by primary key | OK | 330.98 | SELECT ID, NAME FROM USERS WHERE ID = 500 |
| 2 | AdvancedTest | simple select by name | OK | 4.24 | SELECT ID, NAME FROM USERS WHERE NAME = 'User500' |
| 3 | AdvancedTest | complex select with multi-column and conditions | OK | 7.09 | SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500') |
| 4 | AdvancedTest | complex select with or limit offset | OK | 4.76 | SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5 |
| 5 | AliasesTest | simple select with alias order by | OK | 39.26 | SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName |
| 6 | AliasesTest | simple select with as alias order by | OK | 15.87 | SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName |
| 7 | AliasesTest | complex select min max avg with join and group by | OK | 160.63 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC |
| 8 | AliasesTest | complex select with multiple inner joins | OK | 20.56 | SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5 |
| 9 | GroupByTest | simple group by min max avg | OK | 25.15 | SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME |
| 10 | GroupByTest | simple group by sum count | OK | 27.70 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME |
| 11 | GroupByTest | complex group by date having | OK | 48.31 | SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0 |
| 12 | GroupByTest | complex group by join string date | OK | 95.85 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC |
| 13 | InTest | simple in on btree index | OK | 9.30 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) |
| 14 | InTest | simple in on primary key | OK | 1.66 | SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) |
| 15 | InTest | complex in with and | OK | 3.03 | SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000 |
| 16 | InTest | complex in with or | OK | 3.06 | SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000 |
| 17 | JoinTest | simple inner join on primary key | OK | 21.05 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 18 | JoinTest | simple inner join on non indexed field | OK | 19.93 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00 |
| 19 | JoinTest | complex full join on primary key | OK | 24.21 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 20 | JoinTest | complex inner join with and or in on | OK | 31.80 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502) |
| 21 | LikeTest | simple like on name | OK | 15.21 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' |
| 22 | LikeTest | simple like on user code | OK | 11.26 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' |
| 23 | LikeTest | complex like with and | OK | 12.61 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000 |
| 24 | LikeTest | complex like with or | OK | 15.08 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000 |
| 25 | OrderByTest | simple order by name | OK | 18.09 | SELECT ID, NAME FROM USERS ORDER BY NAME |
| 26 | OrderByTest | simple order by age desc | OK | 24.38 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC |
| 27 | PerformanceTest | simple select where age | OK | 4.74 | SELECT NAME, AGE FROM USERS WHERE AGE < 30 |
| 28 | PerformanceTest | simple select clustered index | OK | 2.35 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' |
| 29 | PerformanceTest | complex select age and active | OK | 9.22 | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE |
| 30 | PerformanceTest | complex select parenthesized or | OK | 15.97 | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500 |
| 31 | PersistenceTest | create table | OK | 2.26 | CREATE TABLE PERSIST_TEST (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID) |
| 32 | PersistenceTest | insert alice | OK | 8.29 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000') |
| 33 | PersistenceTest | insert bob full schema | OK | 5.42 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000') |
| 34 | PersistenceTest | select from persisted table | OK | 6.11 | SELECT NAME, AGE FROM PERSIST_TEST WHERE AGE = 25 |
| 35 | SubqueriesTest | simple subquery in in clause | OK | 2012.18 | SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10 |
| 36 | SubqueriesTest | simple subquery in where | FAIL | 9.32 | SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 37 | SubqueriesTest | complex subquery in column where group by having | OK | 14.09 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10 |
| 38 | SubqueriesTest | complex subquery in column inner join on | FAIL | 10.08 | SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 39 | TrueFalseNullTest | create table | OK | 0.94 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 40 | TrueFalseNullTest | insert flag true | OK | 2.86 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 41 | TrueFalseNullTest | insert flag false | OK | 3.99 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 42 | TrueFalseNullTest | insert null in insert | OK | 2.05 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 43 | TrueFalseNullTest | where flag = true | OK | 1.41 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 44 | TrueFalseNullTest | where flag = false | OK | 1.44 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 45 | TrueFalseNullTest | where col is null | OK | 1.62 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 46 | TrueFalseNullTest | where col is not null | OK | 1.12 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 47 | TrueFalseNullTest | where age is null | OK | 1.07 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 48 | TrueFalseNullTest | update set null in update | OK | 3.93 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 49 | TrueFalseNullTest | where col is null after update | OK | 1.71 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 50 | TrueFalseNullTest | where col = null returns empty | OK | 2.01 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 51 | CaseSensitivityTest | create table | OK | 0.83 | CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING) |
| 52 | CaseSensitivityTest | insert john | OK | 1.99 | INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value') |
| 53 | CaseSensitivityTest | where name = 'John' finds row | OK | 1.56 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John' |
| 54 | CaseSensitivityTest | where name = 'JOHN' returns no rows | OK | 1.23 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN' |
| 55 | CaseSensitivityTest | quoted column identifier myColumn | OK | 0.87 | SELECT "myColumn" FROM CASE_TEST |
| 56 | CaseSensitivityTest | create quoted table | OK | 0.59 | CREATE TABLE "MyTable" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING) |
| 57 | CaseSensitivityTest | insert into quoted table | OK | 1.86 | INSERT INTO "MyTable" (NAME) VALUES ('test') |
| 58 | CaseSensitivityTest | select from quoted table | OK | 1.08 | SELECT * FROM "MyTable" |
| 59 | TransactionTest | create table | OK | 4.17 | CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING) |
| 60 | TransactionTest | insert without begin auto-commits | OK | 4.10 | INSERT INTO TXN_TEST (NAME) VALUES ('auto48') |
| 61 | TransactionTest | set autocommit off | OK | 0.64 | SET AUTOCOMMIT = OFF |
| 62 | TransactionTest | set session autocommit on | OK | 0.30 | SET SESSION AUTOCOMMIT = ON |
| 63 | TransactionTest | set session autocommit off | OK | 0.18 | SET SESSION AUTOCOMMIT = OFF |
| 64 | TransactionTest | set autocommit on | OK | 0.17 | SET AUTOCOMMIT = ON |

# AllTestsSampleTest query timings

Generated: Fri Aug 07 12:00:56 GMT+04:00 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | AdvancedTest | simple select by primary key | OK | 230.02 | SELECT ID, NAME FROM USERS WHERE ID = 500 |
| 2 | AdvancedTest | simple select by name | OK | 4.27 | SELECT ID, NAME FROM USERS WHERE NAME = 'User500' |
| 3 | AdvancedTest | complex select with multi-column and conditions | OK | 16.59 | SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500') |
| 4 | AdvancedTest | complex select with or limit offset | OK | 14.14 | SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5 |
| 5 | AliasesTest | simple select with alias order by | OK | 51.72 | SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName |
| 6 | AliasesTest | simple select with as alias order by | OK | 48.81 | SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName |
| 7 | AliasesTest | complex select min max avg with join and group by | OK | 263.56 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC |
| 8 | AliasesTest | complex select with multiple inner joins | OK | 99.80 | SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5 |
| 9 | GroupByTest | simple group by min max avg | OK | 53.45 | SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME |
| 10 | GroupByTest | simple group by sum count | OK | 78.88 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME |
| 11 | GroupByTest | complex group by date having | OK | 65.84 | SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0 |
| 12 | GroupByTest | complex group by join string date | OK | 151.69 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC |
| 13 | InTest | simple in on btree index | OK | 18.09 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) |
| 14 | InTest | simple in on primary key | OK | 1.85 | SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) |
| 15 | InTest | complex in with and | OK | 9.65 | SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000 |
| 16 | InTest | complex in with or | OK | 3.11 | SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000 |
| 17 | JoinTest | simple inner join on primary key | OK | 29.95 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 18 | JoinTest | simple inner join on non indexed field | OK | 23.88 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00 |
| 19 | JoinTest | complex full join on primary key | OK | 34.08 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 20 | JoinTest | complex inner join with and or in on | OK | 28.93 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502) |
| 21 | LikeTest | simple like on name | OK | 34.87 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' |
| 22 | LikeTest | simple like on user code | OK | 34.39 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' |
| 23 | LikeTest | complex like with and | OK | 41.15 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000 |
| 24 | LikeTest | complex like with or | OK | 17.92 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000 |
| 25 | OrderByTest | simple order by name | OK | 56.13 | SELECT ID, NAME FROM USERS ORDER BY NAME |
| 26 | OrderByTest | simple order by age desc | OK | 42.41 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC |
| 27 | PerformanceTest | simple select where age | OK | 4.81 | SELECT NAME, AGE FROM USERS WHERE AGE < 30 |
| 28 | PerformanceTest | simple select clustered index | OK | 17.80 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' |
| 29 | PerformanceTest | complex select age and active | OK | 28.15 | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE |
| 30 | PerformanceTest | complex select parenthesized or | OK | 36.33 | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500 |
| 31 | PersistenceTest | create table | OK | 13.06 | CREATE TABLE PERSIST_TEST (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID) |
| 32 | PersistenceTest | insert alice | OK | 22.84 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000') |
| 33 | PersistenceTest | insert bob full schema | OK | 25.98 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000') |
| 34 | PersistenceTest | select from persisted table | OK | 16.63 | SELECT NAME, AGE FROM PERSIST_TEST WHERE AGE = 25 |
| 35 | SubqueriesTest | simple subquery in in clause | OK | 3333.42 | SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10 |
| 36 | SubqueriesTest | simple subquery in where | OK | 32.32 | SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 37 | SubqueriesTest | complex subquery in column where group by having | OK | 1052.63 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10 |
| 38 | SubqueriesTest | complex subquery in column inner join on | OK | 31.55 | SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 39 | TrueFalseNullTest | create table | OK | 2.96 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 40 | TrueFalseNullTest | insert flag true | OK | 10.53 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 41 | TrueFalseNullTest | insert flag false | OK | 2.45 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 42 | TrueFalseNullTest | insert null in insert | OK | 3.49 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 43 | TrueFalseNullTest | where flag = true | OK | 9.12 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 44 | TrueFalseNullTest | where flag = false | OK | 1.56 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 45 | TrueFalseNullTest | where col is null | OK | 1.50 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 46 | TrueFalseNullTest | where col is not null | OK | 1.00 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 47 | TrueFalseNullTest | where age is null | OK | 1.36 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 48 | TrueFalseNullTest | update set null in update | OK | 16.79 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 49 | TrueFalseNullTest | where col is null after update | OK | 8.82 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 50 | TrueFalseNullTest | where col = null returns empty | OK | 3.19 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 51 | TrueFalseNullTest | where col != null returns empty | OK | 18.13 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL |
| 52 | TrueFalseNullTest | where age < null returns empty | OK | 1.42 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL |
| 53 | TrueFalseNullTest | where age > null returns empty | OK | 1.26 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL |
| 54 | TrueFalseNullTest | where age <= null returns empty | OK | 8.40 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL |
| 55 | TrueFalseNullTest | where age >= null returns empty | OK | 1.73 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL |
| 56 | CaseSensitivityTest | create table | OK | 0.93 | CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING) |
| 57 | CaseSensitivityTest | insert john | OK | 2.82 | INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value') |
| 58 | CaseSensitivityTest | where name = 'John' finds row | OK | 1.78 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John' |
| 59 | CaseSensitivityTest | where name = 'JOHN' returns no rows | OK | 1.42 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN' |
| 60 | CaseSensitivityTest | quoted column identifier myColumn | OK | 10.94 | SELECT "myColumn" FROM CASE_TEST |
| 61 | CaseSensitivityTest | create quoted table | OK | 0.78 | CREATE TABLE "MyTable" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING) |
| 62 | CaseSensitivityTest | insert into quoted table | OK | 2.68 | INSERT INTO "MyTable" (NAME) VALUES ('test') |
| 63 | CaseSensitivityTest | select from quoted table | OK | 0.95 | SELECT * FROM "MyTable" |
| 64 | TransactionTest | create table | OK | 1.03 | CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING) |
| 65 | TransactionTest | insert without begin auto-commits | OK | 2.06 | INSERT INTO TXN_TEST (NAME) VALUES ('auto48') |
| 66 | TransactionTest | set autocommit off | OK | 0.60 | SET AUTOCOMMIT = OFF |
| 67 | TransactionTest | set session autocommit on | OK | 0.24 | SET SESSION AUTOCOMMIT = ON |
| 68 | TransactionTest | set session autocommit off | OK | 0.13 | SET SESSION AUTOCOMMIT = OFF |
| 69 | TransactionTest | set autocommit on | OK | 0.15 | SET AUTOCOMMIT = ON |

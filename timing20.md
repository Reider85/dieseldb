# AllTestsSampleTest query timings

Generated: Thu Aug 06 12:47:31 MSK 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | AdvancedTest | simple select by primary key | OK | 123.48 | SELECT ID, NAME FROM USERS WHERE ID = 500 |
| 2 | AdvancedTest | simple select by name | OK | 3.40 | SELECT ID, NAME FROM USERS WHERE NAME = 'User500' |
| 3 | AdvancedTest | complex select with multi-column and conditions | OK | 7.15 | SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500') |
| 4 | AdvancedTest | complex select with or limit offset | OK | 4.57 | SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5 |
| 5 | AliasesTest | simple select with alias order by | OK | 25.08 | SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName |
| 6 | AliasesTest | simple select with as alias order by | OK | 11.73 | SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName |
| 7 | AliasesTest | complex select min max avg with join and group by | OK | 80.38 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC |
| 8 | AliasesTest | complex select with multiple inner joins | OK | 23.93 | SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5 |
| 9 | GroupByTest | simple group by min max avg | OK | 15.98 | SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME |
| 10 | GroupByTest | simple group by sum count | OK | 17.52 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME |
| 11 | GroupByTest | complex group by date having | OK | 21.34 | SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0 |
| 12 | GroupByTest | complex group by join string date | OK | 70.20 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC |
| 13 | InTest | simple in on btree index | OK | 6.97 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) |
| 14 | InTest | simple in on primary key | OK | 2.18 | SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) |
| 15 | InTest | complex in with and | OK | 4.56 | SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000 |
| 16 | InTest | complex in with or | OK | 4.25 | SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000 |
| 17 | JoinTest | simple inner join on primary key | OK | 18.77 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 18 | JoinTest | simple inner join on non indexed field | OK | 21.26 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00 |
| 19 | JoinTest | complex full join on primary key | OK | 21.65 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 20 | JoinTest | complex inner join with and or in on | OK | 31.39 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502) |
| 21 | LikeTest | simple like on name | OK | 14.63 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' |
| 22 | LikeTest | simple like on user code | OK | 10.87 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' |
| 23 | LikeTest | complex like with and | OK | 11.22 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000 |
| 24 | LikeTest | complex like with or | OK | 12.40 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000 |
| 25 | OrderByTest | simple order by name | OK | 7.92 | SELECT ID, NAME FROM USERS ORDER BY NAME |
| 26 | OrderByTest | simple order by age desc | OK | 6.91 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC |
| 27 | OrderByTest | complex join order by primary key | OK | 4631.00 | SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.USER_ID > 0 OR PROFILES.USER_ID IS NOT NULL ORDER BY USERS.ID |
| 28 | OrderByTest | complex join order by non indexed | OK | 4612.00 | SELECT USERS.ID, USERS.BALANCE, PROFILES.NON_INDEXED FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.NON_INDEXED LIKE 'Non%' OR PROFILES.NON_INDEXED IS NOT NULL ORDER BY USERS.BALANCE |
| 29 | PerformanceTest | simple select where age | OK | 3.77 | SELECT NAME, AGE FROM USERS WHERE AGE < 30 |
| 30 | PerformanceTest | simple select clustered index | OK | 2.24 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' |
| 31 | PerformanceTest | complex select age and active | OK | 4.98 | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE |
| 32 | PerformanceTest | complex select parenthesized or | OK | 10.02 | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500 |
| 33 | PersistenceTest | create table | OK | 1.62 | CREATE TABLE PERSIST_TEST (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID) |
| 34 | PersistenceTest | insert alice | OK | 4.17 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000') |
| 35 | PersistenceTest | insert bob full schema | OK | 3.49 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000') |
| 36 | PersistenceTest | select from persisted table | OK | 4.44 | SELECT NAME, AGE FROM PERSIST_TEST WHERE AGE = 25 |
| 37 | SubqueriesTest | simple subquery in in clause | OK | 1468.42 | SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10 |
| 38 | SubqueriesTest | simple subquery in where | OK | 8.82 | SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 39 | SubqueriesTest | complex subquery in column where group by having | OK | 382.06 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10 |
| 40 | SubqueriesTest | complex subquery in column inner join on | OK | 8.49 | SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 41 | TrueFalseNullTest | create table | OK | 0.71 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 42 | TrueFalseNullTest | insert flag true | OK | 5.54 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 43 | TrueFalseNullTest | insert flag false | OK | 3.23 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 44 | TrueFalseNullTest | insert null in insert | OK | 2.10 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 45 | TrueFalseNullTest | where flag = true | OK | 1.89 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 46 | TrueFalseNullTest | where flag = false | OK | 1.50 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 47 | TrueFalseNullTest | where col is null | OK | 0.94 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 48 | TrueFalseNullTest | where col is not null | OK | 0.76 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 49 | TrueFalseNullTest | where age is null | OK | 0.48 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 50 | TrueFalseNullTest | update set null in update | OK | 3.09 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 51 | TrueFalseNullTest | where col is null after update | OK | 0.82 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 52 | TrueFalseNullTest | where col = null returns empty | OK | 0.72 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 53 | CaseSensitivityTest | create table | OK | 0.46 | CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING) |
| 54 | CaseSensitivityTest | insert john | OK | 1.50 | INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value') |
| 55 | CaseSensitivityTest | where name = 'John' finds row | OK | 0.92 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John' |
| 56 | CaseSensitivityTest | where name = 'JOHN' returns no rows | OK | 0.60 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN' |
| 57 | CaseSensitivityTest | quoted column identifier myColumn | OK | 0.44 | SELECT "myColumn" FROM CASE_TEST |
| 58 | CaseSensitivityTest | create quoted table | OK | 0.39 | CREATE TABLE "MyTable" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING) |
| 59 | CaseSensitivityTest | insert into quoted table | OK | 2.31 | INSERT INTO "MyTable" (NAME) VALUES ('test') |
| 60 | CaseSensitivityTest | select from quoted table | OK | 0.66 | SELECT * FROM "MyTable" |
| 61 | TransactionTest | create table | OK | 0.47 | CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING) |

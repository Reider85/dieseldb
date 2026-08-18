# AllTestsSampleTest query timings

Generated: Sat Aug 08 17:11:02 GMT+04:00 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | AdvancedTest | simple select by primary key | OK | 188.17 | SELECT ID, NAME FROM USERS WHERE ID = 500 |
| 2 | AdvancedTest | simple select by name | OK | 4.54 | SELECT ID, NAME FROM USERS WHERE NAME = 'User500' |
| 3 | AdvancedTest | complex select with multi-column and conditions | OK | 27.71 | SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500') |
| 4 | AdvancedTest | complex select with or limit offset | OK | 6.69 | SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5 |
| 5 | AliasesTest | simple select with alias order by | OK | 24.59 | SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName |
| 6 | AliasesTest | simple select with as alias order by | OK | 19.20 | SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName |
| 7 | AliasesTest | complex select min max avg with join and group by | OK | 126.24 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC |
| 8 | AliasesTest | complex select with multiple inner joins | OK | 40.91 | SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5 |
| 9 | GroupByTest | simple group by min max avg | OK | 32.82 | SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME |
| 10 | GroupByTest | simple group by sum count | OK | 29.85 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME |
| 11 | GroupByTest | complex group by date having | OK | 38.84 | SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0 |
| 12 | GroupByTest | complex group by join string date | OK | 114.89 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC |
| 13 | InTest | simple in on btree index | OK | 11.53 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) |
| 14 | InTest | simple in on primary key | OK | 2.06 | SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) |
| 15 | InTest | complex in with and | OK | 3.70 | SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000 |
| 16 | InTest | complex in with or | OK | 3.71 | SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000 |
| 17 | JoinTest | simple inner join on primary key | OK | 23.81 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 18 | JoinTest | simple inner join on non indexed field | OK | 26.66 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00 |
| 19 | JoinTest | complex full join on primary key | OK | 26.15 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 20 | JoinTest | complex inner join with and or in on | OK | 30.47 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502) |
| 21 | LikeTest | simple like on name | OK | 10.47 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' |
| 22 | LikeTest | simple like on user code | OK | 8.56 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' |
| 23 | LikeTest | complex like with and | OK | 10.26 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000 |
| 24 | LikeTest | complex like with or | OK | 12.33 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000 |
| 25 | OrderByTest | simple order by name | OK | 14.35 | SELECT ID, NAME FROM USERS ORDER BY NAME |
| 26 | OrderByTest | simple order by age desc | OK | 13.50 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC |
| 27 | PerformanceTest | simple select where age | OK | 9.51 | SELECT NAME, AGE FROM USERS WHERE AGE < 30 |
| 28 | PerformanceTest | simple select clustered index | OK | 2.79 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' |
| 29 | PerformanceTest | complex select age and active | OK | 7.80 | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE |
| 30 | PerformanceTest | complex select parenthesized or | OK | 12.05 | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500 |
| 31 | PersistenceTest | create table | OK | 2.63 | CREATE TABLE PERSIST_TEST (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID) |
| 32 | PersistenceTest | insert alice | OK | 5.25 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000') |
| 33 | PersistenceTest | insert bob full schema | OK | 3.35 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000') |
| 34 | PersistenceTest | select from persisted table | OK | 4.71 | SELECT NAME, AGE FROM PERSIST_TEST WHERE AGE = 25 |
| 35 | SubqueriesTest | simple subquery in in clause | OK | 97.97 | SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10 |
| 36 | SubqueriesTest | simple subquery in where | OK | 32.20 | SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 37 | SubqueriesTest | complex subquery in column where group by having | OK | 775.68 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10 |
| 38 | SubqueriesTest | complex subquery in column inner join on | OK | 17.43 | SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 39 | TrueFalseNullTest | create table | OK | 1.09 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 40 | TrueFalseNullTest | insert flag true | OK | 6.80 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 41 | TrueFalseNullTest | insert flag false | OK | 4.60 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 42 | TrueFalseNullTest | insert null in insert | OK | 4.18 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 43 | TrueFalseNullTest | where flag = true | OK | 2.41 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 44 | TrueFalseNullTest | where flag = false | OK | 3.22 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 45 | TrueFalseNullTest | where col is null | OK | 3.80 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 46 | TrueFalseNullTest | where col is not null | OK | 1.73 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 47 | TrueFalseNullTest | where age is null | OK | 1.18 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 48 | TrueFalseNullTest | update set null in update | OK | 4.84 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 49 | TrueFalseNullTest | where col is null after update | OK | 2.03 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 50 | TrueFalseNullTest | where col = null returns empty | OK | 1.98 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 51 | TrueFalseNullTest | where col != null returns empty | OK | 1.44 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL |
| 52 | TrueFalseNullTest | prompt 57 select * where col = null returns empty | OK | 1.47 | SELECT * FROM NULL_TEST WHERE COL = NULL |
| 53 | TrueFalseNullTest | prompt 57 select * where col != null returns empty | OK | 1.19 | SELECT * FROM NULL_TEST WHERE COL != NULL |
| 54 | TrueFalseNullTest | prompt 58 select * where col is null returns rows with null col | OK | 1.37 | SELECT * FROM NULL_TEST WHERE COL IS NULL |
| 55 | TrueFalseNullTest | where age < null returns empty | OK | 1.31 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL |
| 56 | TrueFalseNullTest | where age > null returns empty | OK | 1.20 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL |
| 57 | TrueFalseNullTest | where age <= null returns empty | OK | 1.17 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL |
| 58 | TrueFalseNullTest | where age >= null returns empty | OK | 1.16 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL |
| 59 | TrueFalseNullTest | where col != 'A' excludes null rows | OK | 1.40 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A' |
| 60 | TrueFalseNullTest | where age < 30 excludes null row | OK | 1.31 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30 |
| 61 | TrueFalseNullTest | where age = 25 or col = null keeps only matching row | OK | 1.64 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 62 | TrueFalseNullTest | where col = null and age = 25 returns empty | OK | 1.61 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25 |
| 63 | TrueFalseNullTest | where true and unknown excludes row | OK | 1.57 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL |
| 64 | TrueFalseNullTest | where false and unknown excludes row | OK | 1.60 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL |
| 65 | TrueFalseNullTest | where unknown and unknown excludes row | OK | 2.06 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL |
| 66 | TrueFalseNullTest | where not true and unknown keeps only false row | OK | 2.21 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL) |
| 67 | TrueFalseNullTest | where true or unknown includes row | OK | 1.52 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 68 | TrueFalseNullTest | where false or unknown excludes row | OK | 1.34 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL |
| 69 | TrueFalseNullTest | where unknown or unknown excludes row | OK | 1.50 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL |
| 70 | TrueFalseNullTest | where false or true and unknown or true include rows | OK | 1.71 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL |
| 71 | TrueFalseNullTest | where not true or unknown excludes all rows | OK | 2.11 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL) |
| 72 | TrueFalseNullTest | update where col is null | OK | 2.62 | UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL |
| 73 | TrueFalseNullTest | select after update where col is null | OK | 1.48 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40 |
| 74 | TrueFalseNullTest | update where col is not null | OK | 3.98 | UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL |
| 75 | TrueFalseNullTest | select after update where col is not null | OK | 1.27 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50 |
| 76 | TrueFalseNullTest | delete where col is null | OK | 3.42 | DELETE FROM NULL_TEST WHERE COL IS NULL |
| 77 | TrueFalseNullTest | select after delete where col is null | OK | 0.83 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 78 | TrueFalseNullTest | delete where col is not null | OK | 2.13 | DELETE FROM NULL_TEST WHERE COL IS NOT NULL |
| 79 | TrueFalseNullTest | select after delete where col is not null | OK | 0.77 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 80 | TrueFalseNullTest | reinsert row a for or logic | OK | 1.92 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 81 | TrueFalseNullTest | reinsert row b for or logic | OK | 2.25 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 82 | TrueFalseNullTest | reinsert null row for or logic | OK | 1.92 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 83 | TrueFalseNullTest | update where false or unknown or true | OK | 3.38 | UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL |
| 84 | TrueFalseNullTest | select after update with or unknown | OK | 1.32 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77 |
| 85 | TrueFalseNullTest | delete where true or unknown | OK | 2.71 | DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 86 | TrueFalseNullTest | select after delete with or unknown | OK | 0.77 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 87 | CaseSensitivityTest | create table | OK | 0.77 | CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING) |
| 88 | CaseSensitivityTest | insert john | OK | 1.94 | INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value') |
| 89 | CaseSensitivityTest | where name = 'John' finds row | OK | 1.55 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John' |
| 90 | CaseSensitivityTest | where name = 'JOHN' returns no rows | OK | 1.20 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN' |
| 91 | CaseSensitivityTest | quoted column identifier myColumn | OK | 0.89 | SELECT "myColumn" FROM CASE_TEST |
| 92 | CaseSensitivityTest | create quoted table | OK | 0.71 | CREATE TABLE "MyTable" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING) |
| 93 | CaseSensitivityTest | insert into quoted table | OK | 2.38 | INSERT INTO "MyTable" (NAME) VALUES ('test') |
| 94 | CaseSensitivityTest | select from quoted table | OK | 0.91 | SELECT * FROM "MyTable" |
| 95 | TransactionTest | create table | OK | 0.83 | CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING) |
| 96 | TransactionTest | insert without begin auto-commits | OK | 2.12 | INSERT INTO TXN_TEST (NAME) VALUES ('auto48') |
| 97 | TransactionTest | set autocommit off | OK | 0.70 | SET AUTOCOMMIT = OFF |
| 98 | TransactionTest | set session autocommit on | OK | 0.30 | SET SESSION AUTOCOMMIT = ON |
| 99 | TransactionTest | set session autocommit off | OK | 0.17 | SET SESSION AUTOCOMMIT = OFF |
| 100 | TransactionTest | set autocommit on | OK | 0.20 | SET AUTOCOMMIT = ON |

# AllTestsSampleTest query timings

Generated: Sat Aug 08 18:17:00 GMT+04:00 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | AdvancedTest | simple select by primary key | OK | 182.09 | SELECT ID, NAME FROM USERS WHERE ID = 500 |
| 2 | AdvancedTest | simple select by name | OK | 5.13 | SELECT ID, NAME FROM USERS WHERE NAME = 'User500' |
| 3 | AdvancedTest | complex select with multi-column and conditions | OK | 30.74 | SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE500') AND (AGE = 50) AND (NAME = 'User500') |
| 4 | AdvancedTest | complex select with or limit offset | OK | 6.08 | SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5 |
| 5 | AliasesTest | simple select with alias order by | OK | 31.03 | SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName |
| 6 | AliasesTest | simple select with as alias order by | OK | 15.97 | SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName |
| 7 | AliasesTest | complex select min max avg with join and group by | OK | 111.71 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC |
| 8 | AliasesTest | complex select with multiple inner joins | OK | 25.87 | SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5 |
| 9 | GroupByTest | simple group by min max avg | OK | 30.24 | SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME |
| 10 | GroupByTest | simple group by sum count | OK | 30.76 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME |
| 11 | GroupByTest | complex group by date having | OK | 32.83 | SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0 |
| 12 | GroupByTest | complex group by join string date | OK | 123.83 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC |
| 13 | InTest | simple in on btree index | OK | 9.14 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) |
| 14 | InTest | simple in on primary key | OK | 2.02 | SELECT ID, NAME FROM USERS WHERE ID IN (500, 501, 502) |
| 15 | InTest | complex in with and | OK | 3.47 | SELECT ID, NAME FROM USERS WHERE NAME IN ('User500', 'User501', 'User502') AND BALANCE > 5000 |
| 16 | InTest | complex in with or | OK | 3.18 | SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE500', 'CODE501', 'CODE502') OR BALANCE > 5000 |
| 17 | JoinTest | simple inner join on primary key | OK | 21.67 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 18 | JoinTest | simple inner join on non indexed field | OK | 22.66 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00 |
| 19 | JoinTest | complex full join on primary key | OK | 25.67 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (500, 501, 502) |
| 20 | JoinTest | complex inner join with and or in on | OK | 32.89 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (500, 501, 502) |
| 21 | LikeTest | simple like on name | OK | 10.91 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' |
| 22 | LikeTest | simple like on user code | OK | 8.03 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' |
| 23 | LikeTest | complex like with and | OK | 10.56 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er500' AND NAME LIKE '%User500%' AND NAME LIKE 'User500%' AND BALANCE > 5000 |
| 24 | LikeTest | complex like with or | OK | 12.74 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE500' AND USER_CODE LIKE '%CODE500%' AND USER_CODE LIKE 'CODE500%' OR BALANCE > 5000 |
| 25 | OrderByTest | simple order by name | OK | 17.14 | SELECT ID, NAME FROM USERS ORDER BY NAME |
| 26 | OrderByTest | simple order by age desc | OK | 17.66 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC |
| 27 | PerformanceTest | simple select where age | OK | 7.16 | SELECT NAME, AGE FROM USERS WHERE AGE < 30 |
| 28 | PerformanceTest | simple select clustered index | OK | 3.06 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' |
| 29 | PerformanceTest | complex select age and active | OK | 8.76 | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE |
| 30 | PerformanceTest | complex select parenthesized or | OK | 19.81 | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500 |
| 31 | PersistenceTest | create table | OK | 2.53 | CREATE TABLE PERSIST_TEST (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID) |
| 32 | PersistenceTest | insert alice | OK | 6.09 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000') |
| 33 | PersistenceTest | insert bob full schema | OK | 3.59 | INSERT INTO PERSIST_TEST (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000') |
| 34 | PersistenceTest | select from persisted table | OK | 4.29 | SELECT NAME, AGE FROM PERSIST_TEST WHERE AGE = 25 |
| 35 | SubqueriesTest | simple subquery in in clause | OK | 92.02 | SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10 |
| 36 | SubqueriesTest | simple subquery in where | OK | 26.41 | SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 37 | SubqueriesTest | complex subquery in column where group by having | OK | 799.38 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10 |
| 38 | SubqueriesTest | complex subquery in column inner join on | OK | 18.72 | SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 500 LIMIT 1) LIMIT 10 |
| 39 | TrueFalseNullTest | create table | OK | 1.08 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 40 | TrueFalseNullTest | insert flag true | OK | 2.63 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 41 | TrueFalseNullTest | insert flag false | OK | 2.35 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 42 | TrueFalseNullTest | insert null in insert | OK | 1.90 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 43 | TrueFalseNullTest | where flag = true | OK | 1.43 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 44 | TrueFalseNullTest | where flag = false | OK | 1.10 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 45 | TrueFalseNullTest | where col is null | OK | 1.73 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 46 | TrueFalseNullTest | where col is not null | OK | 1.04 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 47 | TrueFalseNullTest | where age is null | OK | 0.99 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 48 | TrueFalseNullTest | update set null in update | OK | 4.91 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 49 | TrueFalseNullTest | where col is null after update | OK | 2.44 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 50 | TrueFalseNullTest | where col = null returns empty | OK | 1.36 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 51 | TrueFalseNullTest | where col != null returns empty | OK | 1.03 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL |
| 52 | TrueFalseNullTest | prompt 57 select * where col = null returns empty | OK | 1.41 | SELECT * FROM NULL_TEST WHERE COL = NULL |
| 53 | TrueFalseNullTest | prompt 57 select * where col != null returns empty | OK | 1.88 | SELECT * FROM NULL_TEST WHERE COL != NULL |
| 54 | TrueFalseNullTest | prompt 58 select * where col is null returns rows with null col | OK | 2.14 | SELECT * FROM NULL_TEST WHERE COL IS NULL |
| 55 | TrueFalseNullTest | prompt 59 select * where col = 25 or col is null returns value and null rows | OK | 2.63 | SELECT * FROM NULL_TEST WHERE AGE = 25 OR AGE IS NULL |
| 56 | TrueFalseNullTest | prompt 59 select * where col = 25 and col is not null returns only value row | OK | 2.19 | SELECT * FROM NULL_TEST WHERE AGE = 25 AND AGE IS NOT NULL |
| 57 | TrueFalseNullTest | where age < null returns empty | OK | 2.41 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL |
| 58 | TrueFalseNullTest | where age > null returns empty | OK | 2.26 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL |
| 59 | TrueFalseNullTest | where age <= null returns empty | OK | 4.58 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL |
| 60 | TrueFalseNullTest | where age >= null returns empty | OK | 2.22 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL |
| 61 | TrueFalseNullTest | where col != 'A' excludes null rows | OK | 2.68 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A' |
| 62 | TrueFalseNullTest | where age < 30 excludes null row | OK | 2.53 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30 |
| 63 | TrueFalseNullTest | where age = 25 or col = null keeps only matching row | OK | 1.84 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 64 | TrueFalseNullTest | where col = null and age = 25 returns empty | OK | 1.62 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25 |
| 65 | TrueFalseNullTest | where true and unknown excludes row | OK | 1.51 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL |
| 66 | TrueFalseNullTest | where false and unknown excludes row | OK | 1.66 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL |
| 67 | TrueFalseNullTest | where unknown and unknown excludes row | OK | 1.58 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL |
| 68 | TrueFalseNullTest | where not true and unknown keeps only false row | OK | 2.55 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL) |
| 69 | TrueFalseNullTest | where true or unknown includes row | OK | 1.51 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 70 | TrueFalseNullTest | where false or unknown excludes row | OK | 1.62 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL |
| 71 | TrueFalseNullTest | where unknown or unknown excludes row | OK | 1.52 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL |
| 72 | TrueFalseNullTest | where false or true and unknown or true include rows | OK | 1.19 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL |
| 73 | TrueFalseNullTest | where not true or unknown excludes all rows | OK | 1.67 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL) |
| 74 | TrueFalseNullTest | update where col is null | OK | 2.11 | UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL |
| 75 | TrueFalseNullTest | select after update where col is null | OK | 1.16 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40 |
| 76 | TrueFalseNullTest | update where col is not null | OK | 2.05 | UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL |
| 77 | TrueFalseNullTest | select after update where col is not null | OK | 1.84 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50 |
| 78 | TrueFalseNullTest | delete where col is null | OK | 3.89 | DELETE FROM NULL_TEST WHERE COL IS NULL |
| 79 | TrueFalseNullTest | select after delete where col is null | OK | 0.84 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 80 | TrueFalseNullTest | delete where col is not null | OK | 1.95 | DELETE FROM NULL_TEST WHERE COL IS NOT NULL |
| 81 | TrueFalseNullTest | select after delete where col is not null | OK | 0.83 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 82 | TrueFalseNullTest | reinsert row a for or logic | OK | 1.91 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 83 | TrueFalseNullTest | reinsert row b for or logic | OK | 1.72 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 84 | TrueFalseNullTest | reinsert null row for or logic | OK | 1.93 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 85 | TrueFalseNullTest | update where false or unknown or true | OK | 2.89 | UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL |
| 86 | TrueFalseNullTest | select after update with or unknown | OK | 1.35 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77 |
| 87 | TrueFalseNullTest | delete where true or unknown | OK | 2.63 | DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 88 | TrueFalseNullTest | select after delete with or unknown | OK | 0.86 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 89 | TrueFalseNullTest | prompt 60 create agg table | OK | 0.77 | CREATE TABLE AGG_TEST (ID LONG PRIMARY KEY SEQUENCE(agg_test_seq 1 1), AMOUNT INTEGER) |
| 90 | TrueFalseNullTest | prompt 60 insert amount 10 | OK | 2.06 | INSERT INTO AGG_TEST (AMOUNT) VALUES (10) |
| 91 | TrueFalseNullTest | prompt 60 insert amount 20 | OK | 2.00 | INSERT INTO AGG_TEST (AMOUNT) VALUES (20) |
| 92 | TrueFalseNullTest | prompt 60 insert amount null | OK | 1.77 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 93 | TrueFalseNullTest | prompt 60 insert amount 30 | OK | 1.76 | INSERT INTO AGG_TEST (AMOUNT) VALUES (30) |
| 94 | TrueFalseNullTest | prompt 60 insert amount null 2 | OK | 2.02 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 95 | TrueFalseNullTest | prompt 60 select * returns all rows incl nulls | OK | 0.99 | SELECT * FROM AGG_TEST |
| 96 | TrueFalseNullTest | prompt 60 count star counts all rows | OK | 1.47 | SELECT COUNT(*) FROM AGG_TEST |
| 97 | TrueFalseNullTest | prompt 60 count column skips null | OK | 0.95 | SELECT COUNT(AMOUNT) FROM AGG_TEST |
| 98 | TrueFalseNullTest | prompt 60 sum skips null | OK | 1.09 | SELECT SUM(AMOUNT) FROM AGG_TEST |
| 99 | TrueFalseNullTest | prompt 60 avg skips null | OK | 1.10 | SELECT AVG(AMOUNT) FROM AGG_TEST |
| 100 | TrueFalseNullTest | prompt 60 min skips null | OK | 0.93 | SELECT MIN(AMOUNT) FROM AGG_TEST |
| 101 | TrueFalseNullTest | prompt 60 max skips null | OK | 0.93 | SELECT MAX(AMOUNT) FROM AGG_TEST |
| 102 | CaseSensitivityTest | create table | OK | 0.79 | CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING) |
| 103 | CaseSensitivityTest | insert john | OK | 2.25 | INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value') |
| 104 | CaseSensitivityTest | where name = 'John' finds row | OK | 1.89 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John' |
| 105 | CaseSensitivityTest | where name = 'JOHN' returns no rows | OK | 2.09 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN' |
| 106 | CaseSensitivityTest | quoted column identifier myColumn | OK | 1.48 | SELECT "myColumn" FROM CASE_TEST |
| 107 | CaseSensitivityTest | create quoted table | OK | 0.75 | CREATE TABLE "MyTable" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING) |
| 108 | CaseSensitivityTest | insert into quoted table | OK | 2.26 | INSERT INTO "MyTable" (NAME) VALUES ('test') |
| 109 | CaseSensitivityTest | select from quoted table | OK | 1.11 | SELECT * FROM "MyTable" |
| 110 | TransactionTest | create table | OK | 1.81 | CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING) |
| 111 | TransactionTest | insert without begin auto-commits | OK | 3.42 | INSERT INTO TXN_TEST (NAME) VALUES ('auto48') |
| 112 | TransactionTest | set autocommit off | OK | 0.55 | SET AUTOCOMMIT = OFF |
| 113 | TransactionTest | set session autocommit on | OK | 0.22 | SET SESSION AUTOCOMMIT = ON |
| 114 | TransactionTest | set session autocommit off | OK | 0.16 | SET SESSION AUTOCOMMIT = OFF |
| 115 | TransactionTest | set autocommit on | OK | 0.15 | SET AUTOCOMMIT = ON |

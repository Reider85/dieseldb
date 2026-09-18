# AllTestsSampleTest query timings

Generated: Fri Sep 11 08:58:13 GMT+04:00 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | AdvancedTest | simple select by primary key | OK | 33.04 | SELECT ID, NAME FROM USERS WHERE ID = 50 |
| 2 | AdvancedTest | simple select by name | OK | 18.14 | SELECT ID, NAME FROM USERS WHERE NAME = 'User50' |
| 3 | AdvancedTest | complex select with multi-column and conditions | OK | 16.70 | SELECT ID, NAME FROM USERS WHERE (USER_CODE = 'CODE50') AND (AGE = 50) AND (NAME = 'User50') |
| 4 | AdvancedTest | complex select with or limit offset | OK | 8.60 | SELECT ID, NAME FROM USERS WHERE AGE = 50 OR BALANCE > 5000 LIMIT 10 OFFSET 5 |
| 5 | AliasesTest | simple select with alias order by | OK | 12.75 | SELECT NAME userName, USER_CODE code FROM USERS u ORDER BY userName |
| 6 | AliasesTest | simple select with as alias order by | OK | 4.70 | SELECT NAME AS userName, USER_CODE AS code FROM USERS u ORDER BY userName |
| 7 | AliasesTest | complex select min max avg with join and group by | OK | 19.11 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC |
| 8 | AliasesTest | complex select with multiple inner joins | OK | 9.93 | SELECT u.NAME userName, t.AMOUNT transAmount, u2.NAME refName FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID INNER JOIN USERS u2 ON u.ID = u2.ID LIMIT 10 OFFSET 5 |
| 9 | GroupByTest | simple group by min max avg | OK | 16.14 | SELECT NAME, MIN(AGE), MAX(AGE), AVG(AGE) FROM USERS GROUP BY NAME |
| 10 | GroupByTest | simple group by sum count | OK | 19.93 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME |
| 11 | GroupByTest | complex group by date having | OK | 31.13 | SELECT DATE_FIELD, SUM(BALANCE), COUNT(BALANCE) FROM USERS GROUP BY DATE_FIELD HAVING COUNT(*) > 0 |
| 12 | GroupByTest | complex group by join string date | OK | 14.32 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC |
| 13 | InTest | simple in on btree index | OK | 9.27 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52) |
| 14 | InTest | simple in on primary key | OK | 3.84 | SELECT ID, NAME FROM USERS WHERE ID IN (50, 51, 52) |
| 15 | InTest | complex in with and | OK | 20.22 | SELECT ID, NAME FROM USERS WHERE NAME IN ('User50', 'User51', 'User52') AND BALANCE > 5000 |
| 16 | InTest | complex in with or | OK | 22.22 | SELECT ID, NAME FROM USERS WHERE USER_CODE IN ('CODE50', 'CODE51', 'CODE52') OR BALANCE > 5000 |
| 17 | JoinTest | simple inner join on primary key | OK | 169.32 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (50, 51, 52) |
| 18 | JoinTest | simple inner join on non indexed field | OK | 12.38 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.BALANCE = USER_DETAILS.BALANCE WHERE USERS.BALANCE = 5100.00 |
| 19 | JoinTest | complex full join on primary key | OK | 14.82 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS FULL JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (50, 51, 52) |
| 20 | JoinTest | complex inner join with and or in on | OK | 13.90 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON (USERS.ID = USER_DETAILS.USER_ID AND USERS.NAME = USER_DETAILS.NAME) OR (USERS.USER_CODE = USER_DETAILS.USER_CODE) WHERE USERS.ID IN (50, 51, 52) |
| 21 | LikeTest | simple like on name | OK | 26.38 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er50' AND NAME LIKE '%User50%' AND NAME LIKE 'User50%' |
| 22 | LikeTest | simple like on user code | OK | 8.43 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE50' AND USER_CODE LIKE '%CODE50%' AND USER_CODE LIKE 'CODE50%' |
| 23 | LikeTest | complex like with and | OK | 8.78 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er50' AND NAME LIKE '%User50%' AND NAME LIKE 'User50%' AND BALANCE > 5000 |
| 24 | LikeTest | complex like with or | OK | 9.30 | SELECT ID, NAME FROM USERS WHERE USER_CODE LIKE '%ODE50' AND USER_CODE LIKE '%CODE50%' AND USER_CODE LIKE 'CODE50%' OR BALANCE > 5000 |
| 25 | OrderByTest | simple order by name | OK | 7.91 | SELECT ID, NAME FROM USERS ORDER BY NAME |
| 26 | OrderByTest | simple order by age desc | OK | 3.90 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC |
| 27 | OrderByHeavyTest | complex join order by primary key | OK | 13.78 | SELECT USERS.ID, USERS.NAME, PROFILES.PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.USER_ID > 0 OR PROFILES.USER_ID IS NOT NULL ORDER BY USERS.ID |
| 28 | OrderByHeavyTest | complex join order by non indexed | OK | 11.75 | SELECT USERS.ID, USERS.BALANCE, PROFILES.NON_INDEXED FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID AND PROFILES.NON_INDEXED LIKE 'Non%' OR PROFILES.NON_INDEXED IS NOT NULL ORDER BY USERS.BALANCE |
| 29 | PerformanceTest | simple select where age | OK | 5.46 | SELECT NAME, AGE FROM USERS WHERE AGE < 30 |
| 30 | PerformanceTest | simple select clustered index | OK | 3.70 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50' |
| 31 | PerformanceTest | complex select age and active | OK | 5.88 | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE |
| 32 | PerformanceTest | complex select parenthesized or | OK | 8.50 | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AGE < 35 AND ACTIVE = TRUE) OR BALANCE > 500 |
| 33 | SubqueriesTest | simple subquery in in clause | OK | 30.53 | SELECT ID, NAME FROM USERS WHERE ID IN (SELECT ID FROM USERS WHERE AGE > 50) LIMIT 10 |
| 34 | SubqueriesTest | simple subquery in where | OK | 9.78 | SELECT ID, NAME FROM USERS WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) LIMIT 10 |
| 35 | SubqueriesTest | complex subquery in column where group by having | OK | 194.34 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10 |
| 36 | SubqueriesTest | complex subquery in column inner join on | OK | 11.27 | SELECT u.ID, (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name FROM USERS u INNER JOIN USERS u2 ON u.ID = u2.ID AND u.AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) LIMIT 10 |
| 37 | TrueFalseNullTest | create table | OK | 2.93 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 38 | TrueFalseNullTest | insert flag true | OK | 2.40 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 39 | TrueFalseNullTest | insert flag false | OK | 1.02 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 40 | TrueFalseNullTest | insert null in insert | OK | 1.55 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 41 | TrueFalseNullTest | where flag = true | OK | 5.20 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 42 | TrueFalseNullTest | where flag = false | OK | 3.10 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 43 | TrueFalseNullTest | where col is null | OK | 3.44 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 44 | TrueFalseNullTest | where col is not null | OK | 2.42 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 45 | TrueFalseNullTest | where age is null | OK | 2.47 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 46 | TrueFalseNullTest | update set null in update | OK | 4.80 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 47 | TrueFalseNullTest | where col is null after update | OK | 0.58 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 48 | TrueFalseNullTest | where col = null returns empty | OK | 3.21 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 49 | TrueFalseNullTest | where col != null returns empty | OK | 3.07 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL |
| 50 | TrueFalseNullTest | prompt 57 select * where col = null returns empty | OK | 2.75 | SELECT * FROM NULL_TEST WHERE COL = NULL |
| 51 | TrueFalseNullTest | prompt 57 select * where col != null returns empty | OK | 2.59 | SELECT * FROM NULL_TEST WHERE COL != NULL |
| 52 | TrueFalseNullTest | prompt 58 select * where col is null returns rows with null col | OK | 2.46 | SELECT * FROM NULL_TEST WHERE COL IS NULL |
| 53 | TrueFalseNullTest | prompt 59 select * where col = 25 or col is null returns value and null rows | OK | 6.23 | SELECT * FROM NULL_TEST WHERE AGE = 25 OR AGE IS NULL |
| 54 | TrueFalseNullTest | prompt 59 select * where col = 25 and col is not null returns only value row | OK | 6.41 | SELECT * FROM NULL_TEST WHERE AGE = 25 AND AGE IS NOT NULL |
| 55 | TrueFalseNullTest | where age < null returns empty | OK | 5.24 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL |
| 56 | TrueFalseNullTest | where age > null returns empty | OK | 6.12 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL |
| 57 | TrueFalseNullTest | where age <= null returns empty | OK | 5.09 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL |
| 58 | TrueFalseNullTest | where age >= null returns empty | OK | 3.82 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL |
| 59 | TrueFalseNullTest | where col != 'A' excludes null rows | OK | 3.05 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A' |
| 60 | TrueFalseNullTest | where age < 30 excludes null row | OK | 3.36 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30 |
| 61 | TrueFalseNullTest | where age = 25 or col = null keeps only matching row | OK | 4.50 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 62 | TrueFalseNullTest | where col = null and age = 25 returns empty | OK | 4.92 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25 |
| 63 | TrueFalseNullTest | where true and unknown excludes row | OK | 6.26 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL |
| 64 | TrueFalseNullTest | where false and unknown excludes row | OK | 5.80 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL |
| 65 | TrueFalseNullTest | where unknown and unknown excludes row | OK | 5.41 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL |
| 66 | TrueFalseNullTest | where not true and unknown keeps only false row | OK | 5.41 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL) |
| 67 | TrueFalseNullTest | where true or unknown includes row | OK | 0.52 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 68 | TrueFalseNullTest | where false or unknown excludes row | OK | 6.88 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL |
| 69 | TrueFalseNullTest | where unknown or unknown excludes row | OK | 5.11 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL |
| 70 | TrueFalseNullTest | where false or true and unknown or true include rows | OK | 3.87 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL |
| 71 | TrueFalseNullTest | where not true or unknown excludes all rows | OK | 8.15 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL) |
| 72 | TrueFalseNullTest | update where col is null | OK | 7.15 | UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL |
| 73 | TrueFalseNullTest | select after update where col is null | OK | 3.80 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40 |
| 74 | TrueFalseNullTest | update where col is not null | OK | 1.34 | UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL |
| 75 | TrueFalseNullTest | select after update where col is not null | OK | 3.74 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50 |
| 76 | TrueFalseNullTest | delete where col is null | OK | 8.84 | DELETE FROM NULL_TEST WHERE COL IS NULL |
| 77 | TrueFalseNullTest | select after delete where col is null | OK | 1.70 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 78 | TrueFalseNullTest | delete where col is not null | OK | 1.91 | DELETE FROM NULL_TEST WHERE COL IS NOT NULL |
| 79 | TrueFalseNullTest | select after delete where col is not null | OK | 0.41 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 80 | TrueFalseNullTest | reinsert row a for or logic | OK | 0.56 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 81 | TrueFalseNullTest | reinsert row b for or logic | OK | 0.65 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 82 | TrueFalseNullTest | reinsert null row for or logic | OK | 0.59 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 83 | TrueFalseNullTest | update where false or unknown or true | OK | 2.49 | UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL |
| 84 | TrueFalseNullTest | select after update with or unknown | OK | 5.87 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77 |
| 85 | TrueFalseNullTest | delete where true or unknown | OK | 3.52 | DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 86 | TrueFalseNullTest | select after delete with or unknown | OK | 0.53 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 87 | TrueFalseNullTest | prompt 60 create agg table | OK | 1.46 | CREATE TABLE AGG_TEST (ID LONG PRIMARY KEY SEQUENCE(agg_test_seq 1 1), AMOUNT INTEGER) |
| 88 | TrueFalseNullTest | prompt 60 insert amount 10 | OK | 0.64 | INSERT INTO AGG_TEST (AMOUNT) VALUES (10) |
| 89 | TrueFalseNullTest | prompt 60 insert amount 20 | OK | 0.47 | INSERT INTO AGG_TEST (AMOUNT) VALUES (20) |
| 90 | TrueFalseNullTest | prompt 60 insert amount null | OK | 0.43 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 91 | TrueFalseNullTest | prompt 60 insert amount 30 | OK | 0.47 | INSERT INTO AGG_TEST (AMOUNT) VALUES (30) |
| 92 | TrueFalseNullTest | prompt 60 insert amount null 2 | OK | 0.40 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 93 | TrueFalseNullTest | prompt 60 select * returns all rows incl nulls | OK | 1.45 | SELECT * FROM AGG_TEST |
| 94 | TrueFalseNullTest | prompt 60 count star counts all rows | OK | 1.81 | SELECT COUNT(*) FROM AGG_TEST |
| 95 | TrueFalseNullTest | prompt 60 count column skips null | OK | 2.71 | SELECT COUNT(AMOUNT) FROM AGG_TEST |
| 96 | TrueFalseNullTest | prompt 60 sum skips null | OK | 1.51 | SELECT SUM(AMOUNT) FROM AGG_TEST |
| 97 | TrueFalseNullTest | prompt 60 avg skips null | OK | 1.50 | SELECT AVG(AMOUNT) FROM AGG_TEST |
| 98 | TrueFalseNullTest | prompt 60 min skips null | OK | 1.21 | SELECT MIN(AMOUNT) FROM AGG_TEST |
| 99 | TrueFalseNullTest | prompt 60 max skips null | OK | 1.39 | SELECT MAX(AMOUNT) FROM AGG_TEST |
| 100 | CaseSensitivityTest | create table | OK | 1.67 | CREATE TABLE CASE_TEST (ID LONG PRIMARY KEY SEQUENCE(case_test_seq 1 1), NAME STRING, myColumn STRING) |
| 101 | CaseSensitivityTest | insert john | OK | 0.70 | INSERT INTO CASE_TEST (NAME, myColumn) VALUES ('John', 'value') |
| 102 | CaseSensitivityTest | where name = 'John' finds row | OK | 3.21 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'John' |
| 103 | CaseSensitivityTest | where name = 'JOHN' returns no rows | OK | 2.82 | SELECT ID, NAME FROM CASE_TEST WHERE NAME = 'JOHN' |
| 104 | CaseSensitivityTest | quoted column identifier myColumn | OK | 2.13 | SELECT "myColumn" FROM CASE_TEST |
| 105 | CaseSensitivityTest | create quoted table | OK | 1.97 | CREATE TABLE "MyTable" (ID LONG PRIMARY KEY SEQUENCE(mytable_seq 1 1), NAME STRING) |
| 106 | CaseSensitivityTest | insert into quoted table | OK | 0.81 | INSERT INTO "MyTable" (NAME) VALUES ('test') |
| 107 | CaseSensitivityTest | select from quoted table | OK | 1.33 | SELECT * FROM "MyTable" |
| 108 | TransactionTest | create table | OK | 1.59 | CREATE TABLE TXN_TEST (ID LONG PRIMARY KEY SEQUENCE(txn_seq 1 1), NAME STRING) |
| 109 | TransactionTest | insert without begin auto-commits | OK | 0.80 | INSERT INTO TXN_TEST (NAME) VALUES ('auto48') |
| 110 | TransactionTest | set autocommit off | OK | 0.50 | SET AUTOCOMMIT = OFF |
| 111 | TransactionTest | set session autocommit on | OK | 0.18 | SET SESSION AUTOCOMMIT = ON |
| 112 | TransactionTest | set session autocommit off | OK | 0.19 | SET SESSION AUTOCOMMIT = OFF |
| 113 | TransactionTest | set autocommit on | OK | 0.14 | SET AUTOCOMMIT = ON |
| 114 | Prompt62Test | prompt 62 create users table | OK | 1.29 | CREATE TABLE USERS (ID INTEGER, NAME STRING) |
| 115 | Prompt62Test | prompt 62 insert John | OK | 0.69 | INSERT INTO USERS (ID, NAME) VALUES (1, 'John') |
| 116 | Prompt62Test | prompt 62 insert jane | OK | 0.50 | INSERT INTO USERS (ID, NAME) VALUES (2, 'jane') |
| 117 | Prompt62Test | prompt 62 where name = 'John' returns only the John row | OK | 3.52 | SELECT * FROM USERS WHERE NAME = 'John' |
| 118 | Prompt62Test | prompt 63 where name = 'JOHN' returns no rows | OK | 3.02 | SELECT * FROM USERS WHERE NAME = 'JOHN' |
| 119 | Prompt62Test | prompt 63 where name = 'John' returns the John row | OK | 3.64 | SELECT * FROM USERS WHERE NAME = 'John' |
| 120 | Prompt62Test | prompt 64 insert null name | OK | 0.55 | INSERT INTO USERS (ID, NAME) VALUES (3, NULL) |
| 121 | Prompt62Test | prompt 64 where name is null returns only the null name row | OK | 2.87 | SELECT * FROM USERS WHERE NAME IS NULL |
| 122 | Prompt62Test | prompt 64 where name = null returns no rows | OK | 3.07 | SELECT * FROM USERS WHERE NAME = NULL |
| 123 | Prompt65Test | prompt 65 create bool table | OK | 1.51 | CREATE TABLE BOOL_TEST (ID LONG PRIMARY KEY SEQUENCE(bool_test_seq 1 1), FLAG BOOLEAN) |
| 124 | Prompt65Test | prompt 65 insert flag true | OK | 1.49 | INSERT INTO BOOL_TEST (FLAG) VALUES (TRUE) |
| 125 | Prompt65Test | prompt 65 insert flag false | OK | 0.85 | INSERT INTO BOOL_TEST (FLAG) VALUES (FALSE) |
| 126 | Prompt65Test | prompt 65 where flag = true returns only the true row | OK | 6.82 | SELECT * FROM BOOL_TEST WHERE FLAG = TRUE |
| 127 | Prompt65Test | prompt 65 where flag = false returns only the false row | OK | 4.69 | SELECT * FROM BOOL_TEST WHERE FLAG = FALSE |
| 128 | Prompt66Test | prompt 66 create transaction table | OK | 1.71 | CREATE TABLE TXN66_TEST (ID LONG PRIMARY KEY SEQUENCE(txn66_seq 1 1), NAME STRING) |
| 129 | Prompt66Test | prompt 66 insert without begin auto-commits | OK | 0.75 | INSERT INTO TXN66_TEST (NAME) VALUES ('prompt66-auto') |
| 130 | Prompt67Test | prompt 67 create transaction table | OK | 2.34 | CREATE TABLE TXN67_TEST (ID LONG PRIMARY KEY SEQUENCE(txn67_seq 1 1), NAME STRING) |
| 131 | Prompt67Test | prompt 67 select before commit is isolated from the transaction | OK | 0.72 | SELECT * FROM TXN67_TEST WHERE NAME = 'prompt67-first' |
| 132 | Prompt67Test | prompt 67 both rows visible only after COMMIT | OK | 0.60 | SELECT * FROM TXN67_TEST |
| 133 | Prompt68Test | prompt 68 create multi-client table | OK | 1.44 | CREATE TABLE TXN68_TEST (ID LONG PRIMARY KEY SEQUENCE(txn68_seq 1 1), CLIENT STRING, NAME STRING) |
| 134 | Prompt68Test | prompt 68 reader sees the other client's committed row | OK | 7.85 | SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-committed' |
| 135 | Prompt68Test | prompt 68 reader does not see the other client's uncommitted row | OK | 7.43 | SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted' |
| 136 | Prompt68Test | prompt 68 reader sees the row after the writer's COMMIT | OK | 0.89 | SELECT * FROM TXN68_TEST WHERE NAME = 'prompt68-uncommitted' |

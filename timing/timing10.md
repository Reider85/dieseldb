# AllTestsSampleTest query timings

Generated: Wed Sep 09 21:22:46 GMT+04:00 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | TrueFalseNullTest | create table | OK | 3.51 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 2 | TrueFalseNullTest | insert flag true | OK | 2.83 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 3 | TrueFalseNullTest | insert flag false | OK | 2.30 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 4 | TrueFalseNullTest | insert null in insert | FAIL | 1.71 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 5 | TrueFalseNullTest | where flag = true | OK | 165.58 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 6 | TrueFalseNullTest | where flag = false | OK | 12.40 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 7 | TrueFalseNullTest | where col is null | FAIL | 8.91 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 8 | TrueFalseNullTest | where col is not null | OK | 16.64 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 9 | TrueFalseNullTest | where age is null | FAIL | 11.00 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 10 | TrueFalseNullTest | update set null in update | OK | 6.09 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 11 | TrueFalseNullTest | where col is null after update | FAIL | 7.48 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 12 | TrueFalseNullTest | where col = null returns empty | OK | 6.57 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 13 | TrueFalseNullTest | where col != null returns empty | OK | 12.21 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL |
| 14 | TrueFalseNullTest | prompt 57 select * where col = null returns empty | OK | 4.14 | SELECT * FROM NULL_TEST WHERE COL = NULL |
| 15 | TrueFalseNullTest | prompt 57 select * where col != null returns empty | OK | 10.21 | SELECT * FROM NULL_TEST WHERE COL != NULL |
| 16 | TrueFalseNullTest | prompt 58 select * where col is null returns rows with null col | FAIL | 5.19 | SELECT * FROM NULL_TEST WHERE COL IS NULL |
| 17 | TrueFalseNullTest | prompt 59 select * where col = 25 or col is null returns value and null rows | FAIL | 12.45 | SELECT * FROM NULL_TEST WHERE AGE = 25 OR AGE IS NULL |
| 18 | TrueFalseNullTest | prompt 59 select * where col = 25 and col is not null returns only value row | OK | 6.93 | SELECT * FROM NULL_TEST WHERE AGE = 25 AND AGE IS NOT NULL |
| 19 | TrueFalseNullTest | where age < null returns empty | OK | 11.68 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL |
| 20 | TrueFalseNullTest | where age > null returns empty | OK | 5.74 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL |
| 21 | TrueFalseNullTest | where age <= null returns empty | OK | 9.92 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL |
| 22 | TrueFalseNullTest | where age >= null returns empty | OK | 17.10 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL |
| 23 | TrueFalseNullTest | where col != 'A' excludes null rows | OK | 18.83 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A' |
| 24 | TrueFalseNullTest | where age < 30 excludes null row | OK | 4.89 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30 |
| 25 | TrueFalseNullTest | where age = 25 or col = null keeps only matching row | OK | 15.61 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 26 | TrueFalseNullTest | where col = null and age = 25 returns empty | OK | 14.14 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25 |
| 27 | TrueFalseNullTest | where true and unknown excludes row | OK | 12.24 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL |
| 28 | TrueFalseNullTest | where false and unknown excludes row | OK | 7.19 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL |
| 29 | TrueFalseNullTest | where unknown and unknown excludes row | OK | 11.29 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL |
| 30 | TrueFalseNullTest | where not true and unknown keeps only false row | OK | 18.46 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL) |
| 31 | TrueFalseNullTest | where true or unknown includes row | OK | 0.41 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 32 | TrueFalseNullTest | where false or unknown excludes row | OK | 11.44 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL |
| 33 | TrueFalseNullTest | where unknown or unknown excludes row | OK | 5.67 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL |
| 34 | TrueFalseNullTest | where false or true and unknown or true include rows | FAIL | 10.67 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL |
| 35 | TrueFalseNullTest | where not true or unknown excludes all rows | OK | 6.53 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL) |
| 36 | TrueFalseNullTest | update where col is null | OK | 14.83 | UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL |
| 37 | TrueFalseNullTest | select after update where col is null | FAIL | 14.76 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40 |
| 38 | TrueFalseNullTest | update where col is not null | OK | 13.03 | UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL |
| 39 | TrueFalseNullTest | select after update where col is not null | OK | 3.22 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50 |
| 40 | TrueFalseNullTest | delete where col is null | OK | 17.93 | DELETE FROM NULL_TEST WHERE COL IS NULL |
| 41 | TrueFalseNullTest | select after delete where col is null | OK | 3.43 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 42 | TrueFalseNullTest | delete where col is not null | OK | 14.17 | DELETE FROM NULL_TEST WHERE COL IS NOT NULL |
| 43 | TrueFalseNullTest | select after delete where col is not null | OK | 0.57 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 44 | TrueFalseNullTest | reinsert row a for or logic | OK | 2.52 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 45 | TrueFalseNullTest | reinsert row b for or logic | OK | 11.65 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 46 | TrueFalseNullTest | reinsert null row for or logic | FAIL | 0.63 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 47 | TrueFalseNullTest | update where false or unknown or true | OK | 14.08 | UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL |
| 48 | TrueFalseNullTest | select after update with or unknown | FAIL | 3.96 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77 |
| 49 | TrueFalseNullTest | delete where true or unknown | OK | 12.67 | DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 50 | TrueFalseNullTest | select after delete with or unknown | FAIL | 0.61 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 51 | TrueFalseNullTest | prompt 60 create agg table | OK | 0.78 | CREATE TABLE AGG_TEST (ID LONG PRIMARY KEY SEQUENCE(agg_test_seq 1 1), AMOUNT INTEGER) |
| 52 | TrueFalseNullTest | prompt 60 insert amount 10 | OK | 1.74 | INSERT INTO AGG_TEST (AMOUNT) VALUES (10) |
| 53 | TrueFalseNullTest | prompt 60 insert amount 20 | OK | 1.63 | INSERT INTO AGG_TEST (AMOUNT) VALUES (20) |
| 54 | TrueFalseNullTest | prompt 60 insert amount null | FAIL | 0.53 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 55 | TrueFalseNullTest | prompt 60 insert amount 30 | OK | 8.76 | INSERT INTO AGG_TEST (AMOUNT) VALUES (30) |
| 56 | TrueFalseNullTest | prompt 60 insert amount null 2 | FAIL | 0.88 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 57 | TrueFalseNullTest | prompt 60 select * returns all rows incl nulls | FAIL | 1.38 | SELECT * FROM AGG_TEST |
| 58 | TrueFalseNullTest | prompt 60 count star counts all rows | FAIL | 84.67 | SELECT COUNT(*) FROM AGG_TEST |
| 59 | TrueFalseNullTest | prompt 60 count column skips null | OK | 1.45 | SELECT COUNT(AMOUNT) FROM AGG_TEST |
| 60 | TrueFalseNullTest | prompt 60 sum skips null | OK | 2.29 | SELECT SUM(AMOUNT) FROM AGG_TEST |
| 61 | TrueFalseNullTest | prompt 60 avg skips null | OK | 1.15 | SELECT AVG(AMOUNT) FROM AGG_TEST |
| 62 | TrueFalseNullTest | prompt 60 min skips null | OK | 2.95 | SELECT MIN(AMOUNT) FROM AGG_TEST |
| 63 | TrueFalseNullTest | prompt 60 max skips null | OK | 1.70 | SELECT MAX(AMOUNT) FROM AGG_TEST |
| 64 | Prompt62Test | prompt 62 create users table | OK | 0.74 | CREATE TABLE USERS (ID INTEGER, NAME STRING) |
| 65 | Prompt62Test | prompt 62 insert John | OK | 1.87 | INSERT INTO USERS (ID, NAME) VALUES (1, 'John') |
| 66 | Prompt62Test | prompt 62 insert jane | OK | 1.40 | INSERT INTO USERS (ID, NAME) VALUES (2, 'jane') |
| 67 | Prompt62Test | prompt 62 where name = 'John' returns only the John row | OK | 2.07 | SELECT * FROM USERS WHERE NAME = 'John' |
| 68 | Prompt62Test | prompt 63 where name = 'JOHN' returns no rows | OK | 2.55 | SELECT * FROM USERS WHERE NAME = 'JOHN' |
| 69 | Prompt62Test | prompt 63 where name = 'John' returns the John row | OK | 4.91 | SELECT * FROM USERS WHERE NAME = 'John' |
| 70 | Prompt62Test | prompt 64 insert null name | FAIL | 0.62 | INSERT INTO USERS (ID, NAME) VALUES (3, NULL) |
| 71 | Prompt62Test | prompt 64 where name is null returns only the null name row | FAIL | 5.02 | SELECT * FROM USERS WHERE NAME IS NULL |
| 72 | Prompt62Test | prompt 64 where name = null returns no rows | OK | 2.69 | SELECT * FROM USERS WHERE NAME = NULL |

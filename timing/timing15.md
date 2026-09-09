# AllTestsSampleTest query timings

Generated: Wed Sep 09 21:31:38 GMT+04:00 2026

| # | Group | Test | Result | Time (ms) | Query |
|---|-------|------|--------|-----------|-------|
| 1 | TrueFalseNullTest | create table | OK | 1.89 | CREATE TABLE NULL_TEST (ID LONG PRIMARY KEY SEQUENCE(null_test_seq 1 1), FLAG BOOLEAN, COL STRING, AGE INTEGER) |
| 2 | TrueFalseNullTest | insert flag true | OK | 2.22 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 3 | TrueFalseNullTest | insert flag false | OK | 2.21 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 4 | TrueFalseNullTest | insert null in insert | FAIL | 1.91 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 5 | TrueFalseNullTest | where flag = true | OK | 177.23 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = TRUE |
| 6 | TrueFalseNullTest | where flag = false | OK | 7.57 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE FLAG = FALSE |
| 7 | TrueFalseNullTest | where col is null | FAIL | 5.05 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 8 | TrueFalseNullTest | where col is not null | OK | 5.68 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NOT NULL |
| 9 | TrueFalseNullTest | where age is null | FAIL | 4.17 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE IS NULL |
| 10 | TrueFalseNullTest | update set null in update | OK | 6.28 | UPDATE NULL_TEST SET COL = NULL WHERE ID = 1 |
| 11 | TrueFalseNullTest | where col is null after update | FAIL | 0.70 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL IS NULL |
| 12 | TrueFalseNullTest | where col = null returns empty | OK | 7.29 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL |
| 13 | TrueFalseNullTest | where col != null returns empty | OK | 8.82 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != NULL |
| 14 | TrueFalseNullTest | prompt 57 select * where col = null returns empty | OK | 6.05 | SELECT * FROM NULL_TEST WHERE COL = NULL |
| 15 | TrueFalseNullTest | prompt 57 select * where col != null returns empty | OK | 5.72 | SELECT * FROM NULL_TEST WHERE COL != NULL |
| 16 | TrueFalseNullTest | prompt 58 select * where col is null returns rows with null col | FAIL | 7.54 | SELECT * FROM NULL_TEST WHERE COL IS NULL |
| 17 | TrueFalseNullTest | prompt 59 select * where col = 25 or col is null returns value and null rows | FAIL | 7.49 | SELECT * FROM NULL_TEST WHERE AGE = 25 OR AGE IS NULL |
| 18 | TrueFalseNullTest | prompt 59 select * where col = 25 and col is not null returns only value row | OK | 6.12 | SELECT * FROM NULL_TEST WHERE AGE = 25 AND AGE IS NOT NULL |
| 19 | TrueFalseNullTest | where age < null returns empty | OK | 4.85 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < NULL |
| 20 | TrueFalseNullTest | where age > null returns empty | OK | 4.66 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE > NULL |
| 21 | TrueFalseNullTest | where age <= null returns empty | OK | 4.66 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE <= NULL |
| 22 | TrueFalseNullTest | where age >= null returns empty | OK | 4.28 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE >= NULL |
| 23 | TrueFalseNullTest | where col != 'A' excludes null rows | OK | 7.07 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL != 'A' |
| 24 | TrueFalseNullTest | where age < 30 excludes null row | OK | 15.91 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE < 30 |
| 25 | TrueFalseNullTest | where age = 25 or col = null keeps only matching row | OK | 13.36 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 26 | TrueFalseNullTest | where col = null and age = 25 returns empty | OK | 7.59 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = 25 |
| 27 | TrueFalseNullTest | where true and unknown excludes row | OK | 5.64 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 AND COL = NULL |
| 28 | TrueFalseNullTest | where false and unknown excludes row | OK | 6.38 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 30 AND COL = NULL |
| 29 | TrueFalseNullTest | where unknown and unknown excludes row | OK | 5.69 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL AND AGE = NULL |
| 30 | TrueFalseNullTest | where not true and unknown keeps only false row | OK | 7.40 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 AND COL = NULL) |
| 31 | TrueFalseNullTest | where true or unknown includes row | OK | 0.51 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 32 | TrueFalseNullTest | where false or unknown excludes row | OK | 5.79 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL = NULL |
| 33 | TrueFalseNullTest | where unknown or unknown excludes row | OK | 49.94 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE COL = NULL OR AGE = NULL |
| 34 | TrueFalseNullTest | where false or true and unknown or true include rows | FAIL | 4.76 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 99 OR COL IS NULL |
| 35 | TrueFalseNullTest | where not true or unknown excludes all rows | OK | 8.14 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE NOT (AGE = 25 OR COL = NULL) |
| 36 | TrueFalseNullTest | update where col is null | OK | 14.17 | UPDATE NULL_TEST SET AGE = 40 WHERE COL IS NULL |
| 37 | TrueFalseNullTest | select after update where col is null | FAIL | 6.04 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 40 |
| 38 | TrueFalseNullTest | update where col is not null | OK | 5.04 | UPDATE NULL_TEST SET AGE = 50 WHERE COL IS NOT NULL |
| 39 | TrueFalseNullTest | select after update where col is not null | OK | 7.20 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 50 |
| 40 | TrueFalseNullTest | delete where col is null | OK | 14.59 | DELETE FROM NULL_TEST WHERE COL IS NULL |
| 41 | TrueFalseNullTest | select after delete where col is null | OK | 2.62 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 42 | TrueFalseNullTest | delete where col is not null | OK | 4.12 | DELETE FROM NULL_TEST WHERE COL IS NOT NULL |
| 43 | TrueFalseNullTest | select after delete where col is not null | OK | 0.45 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 44 | TrueFalseNullTest | reinsert row a for or logic | OK | 1.94 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (TRUE, 'A', 25) |
| 45 | TrueFalseNullTest | reinsert row b for or logic | OK | 2.04 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (FALSE, 'B', 30) |
| 46 | TrueFalseNullTest | reinsert null row for or logic | FAIL | 0.60 | INSERT INTO NULL_TEST (FLAG, COL, AGE) VALUES (NULL, NULL, NULL) |
| 47 | TrueFalseNullTest | update where false or unknown or true | OK | 3.97 | UPDATE NULL_TEST SET AGE = 77 WHERE AGE = 99 OR COL IS NULL |
| 48 | TrueFalseNullTest | select after update with or unknown | FAIL | 42.24 | SELECT ID, FLAG, COL FROM NULL_TEST WHERE AGE = 77 |
| 49 | TrueFalseNullTest | delete where true or unknown | OK | 6.33 | DELETE FROM NULL_TEST WHERE AGE = 25 OR COL = NULL |
| 50 | TrueFalseNullTest | select after delete with or unknown | FAIL | 0.58 | SELECT ID, FLAG, COL FROM NULL_TEST |
| 51 | TrueFalseNullTest | prompt 60 create agg table | OK | 0.61 | CREATE TABLE AGG_TEST (ID LONG PRIMARY KEY SEQUENCE(agg_test_seq 1 1), AMOUNT INTEGER) |
| 52 | TrueFalseNullTest | prompt 60 insert amount 10 | OK | 1.95 | INSERT INTO AGG_TEST (AMOUNT) VALUES (10) |
| 53 | TrueFalseNullTest | prompt 60 insert amount 20 | OK | 2.48 | INSERT INTO AGG_TEST (AMOUNT) VALUES (20) |
| 54 | TrueFalseNullTest | prompt 60 insert amount null | FAIL | 0.59 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 55 | TrueFalseNullTest | prompt 60 insert amount 30 | OK | 1.84 | INSERT INTO AGG_TEST (AMOUNT) VALUES (30) |
| 56 | TrueFalseNullTest | prompt 60 insert amount null 2 | FAIL | 0.57 | INSERT INTO AGG_TEST (AMOUNT) VALUES (NULL) |
| 57 | TrueFalseNullTest | prompt 60 select * returns all rows incl nulls | FAIL | 1.55 | SELECT * FROM AGG_TEST |
| 58 | TrueFalseNullTest | prompt 60 count star counts all rows | FAIL | 56.65 | SELECT COUNT(*) FROM AGG_TEST |
| 59 | TrueFalseNullTest | prompt 60 count column skips null | OK | 1.49 | SELECT COUNT(AMOUNT) FROM AGG_TEST |
| 60 | TrueFalseNullTest | prompt 60 sum skips null | OK | 2.64 | SELECT SUM(AMOUNT) FROM AGG_TEST |
| 61 | TrueFalseNullTest | prompt 60 avg skips null | OK | 1.31 | SELECT AVG(AMOUNT) FROM AGG_TEST |
| 62 | TrueFalseNullTest | prompt 60 min skips null | OK | 0.92 | SELECT MIN(AMOUNT) FROM AGG_TEST |
| 63 | TrueFalseNullTest | prompt 60 max skips null | OK | 1.06 | SELECT MAX(AMOUNT) FROM AGG_TEST |
| 64 | Prompt62Test | prompt 62 create users table | OK | 0.82 | CREATE TABLE USERS (ID INTEGER, NAME STRING) |
| 65 | Prompt62Test | prompt 62 insert John | OK | 4.60 | INSERT INTO USERS (ID, NAME) VALUES (1, 'John') |
| 66 | Prompt62Test | prompt 62 insert jane | OK | 2.83 | INSERT INTO USERS (ID, NAME) VALUES (2, 'jane') |
| 67 | Prompt62Test | prompt 62 where name = 'John' returns only the John row | OK | 4.00 | SELECT * FROM USERS WHERE NAME = 'John' |
| 68 | Prompt62Test | prompt 63 where name = 'JOHN' returns no rows | OK | 2.67 | SELECT * FROM USERS WHERE NAME = 'JOHN' |
| 69 | Prompt62Test | prompt 63 where name = 'John' returns the John row | OK | 2.19 | SELECT * FROM USERS WHERE NAME = 'John' |
| 70 | Prompt62Test | prompt 64 insert null name | FAIL | 0.46 | INSERT INTO USERS (ID, NAME) VALUES (3, NULL) |
| 71 | Prompt62Test | prompt 64 where name is null returns only the null name row | FAIL | 2.03 | SELECT * FROM USERS WHERE NAME IS NULL |
| 72 | Prompt62Test | prompt 64 where name = null returns no rows | OK | 2.67 | SELECT * FROM USERS WHERE NAME = NULL |

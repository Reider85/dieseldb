# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     18,795 |     14,107 |     28,492 |      4,777 |
| UPDATE               | 10 records                                         |     52,007 |     35,077 |    116,860 |     23,381 |
| TRANSACTION          | 10 records                                         |     67,729 |     51,259 |    102,721 |     16,472 |
| READ_UNCOMMITTED     | 10 records                                         |     64,153 |     57,908 |     75,948 |      6,990 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      1,167 |      0,605 |      5,443 |      1,429 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,771 |      0,690 |      0,905 |      0,071 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,721 |      0,677 |      0,807 |      0,038 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      1,079 |      0,966 |      1,270 |      0,087 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,998 |      0,907 |      1,168 |      0,086 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,935 |      0,766 |      1,079 |      0,084 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      1,726 |      1,589 |      2,211 |      0,176 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,767 |      1,628 |      2,124 |      0,143 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,942 |      0,891 |      1,063 |      0,050 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,925 |      1,012 |      9,007 |      2,362 |

# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     12,619 |     10,790 |     17,201 |      1,855 |
| UPDATE               | 10 records                                         |     22,482 |     18,747 |     25,953 |      2,146 |
| TRANSACTION          | 10 records                                         |     34,226 |     29,425 |     39,635 |      3,572 |
| READ_UNCOMMITTED     | 10 records                                         |     58,259 |     55,451 |     60,630 |      1,600 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,055 |      0,042 |      0,093 |      0,015 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,048 |      0,045 |      0,061 |      0,005 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,049 |      0,047 |      0,057 |      0,003 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,049 |      0,046 |      0,056 |      0,003 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,048 |      0,042 |      0,063 |      0,006 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,044 |      0,042 |      0,051 |      0,003 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,052 |      0,047 |      0,067 |      0,006 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,082 |      0,051 |      0,147 |      0,041 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,065 |      0,054 |      0,107 |      0,015 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,059 |      0,054 |      0,067 |      0,004 |

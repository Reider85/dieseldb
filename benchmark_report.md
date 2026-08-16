# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     24,463 |     13,110 |     46,839 |      9,247 |
| UPDATE               | 10 records                                         |     26,307 |     23,907 |     31,260 |      2,000 |
| TRANSACTION          | 10 records                                         |     52,251 |     43,655 |     60,382 |      6,278 |
| READ_UNCOMMITTED     | 10 records                                         |     60,300 |     57,172 |     71,045 |      3,746 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,058 |      0,052 |      0,085 |      0,010 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,061 |      0,053 |      0,113 |      0,018 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,059 |      0,055 |      0,070 |      0,005 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,102 |      0,060 |      0,426 |      0,108 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,063 |      0,056 |      0,083 |      0,010 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,059 |      0,056 |      0,071 |      0,005 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,066 |      0,062 |      0,088 |      0,007 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,142 |      0,066 |      0,499 |      0,129 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,058 |      0,054 |      0,072 |      0,005 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,105 |      0,057 |      0,445 |      0,114 |

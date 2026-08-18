# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     19,197 |     12,971 |     29,867 |      4,898 |
| UPDATE               | 10 records                                         |     38,140 |     27,280 |     78,774 |     14,690 |
| TRANSACTION          | 10 records                                         |     81,159 |     45,422 |    180,740 |     42,236 |
| READ_UNCOMMITTED     | 10 records                                         |     84,430 |     57,504 |    178,131 |     38,466 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,054 |      0,050 |      0,078 |      0,008 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,055 |      0,052 |      0,070 |      0,005 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,082 |      0,056 |      0,201 |      0,046 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,222 |      0,063 |      0,967 |      0,275 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,119 |      0,059 |      0,426 |      0,107 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,059 |      0,057 |      0,071 |      0,004 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,069 |      0,065 |      0,086 |      0,006 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,109 |      0,069 |      0,370 |      0,089 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,061 |      0,058 |      0,075 |      0,005 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,066 |      0,060 |      0,094 |      0,010 |

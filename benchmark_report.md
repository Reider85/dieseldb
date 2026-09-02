# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     16,030 |     12,437 |     21,743 |      3,321 |
| UPDATE               | 10 records                                         |     31,723 |     25,285 |     41,332 |      4,251 |
| TRANSACTION          | 10 records                                         |     45,257 |     37,145 |     61,323 |      8,197 |
| READ_UNCOMMITTED     | 10 records                                         |     62,509 |     59,016 |     77,918 |      5,250 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,048 |      0,044 |      0,066 |      0,006 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,049 |      0,045 |      0,061 |      0,006 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,054 |      0,050 |      0,065 |      0,004 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,082 |      0,072 |      0,106 |      0,012 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,061 |      0,054 |      0,077 |      0,008 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,118 |      0,050 |      0,592 |      0,159 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,054 |      0,049 |      0,075 |      0,008 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,061 |      0,054 |      0,095 |      0,012 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,066 |      0,059 |      0,111 |      0,015 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,088 |      0,054 |      0,301 |      0,072 |

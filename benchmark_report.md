# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    144,456 |    111,495 |    169,562 |     18,399 |
| UPDATE               | 10 records                                         |    139,022 |    115,292 |    194,335 |     20,847 |
| TRANSACTION          | 10 records                                         |    400,088 |    275,658 |    520,243 |     74,622 |
| READ_UNCOMMITTED     | 10 records                                         |    123,307 |    116,296 |    134,527 |      4,951 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,393 |      0,263 |      0,989 |      0,204 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,272 |      0,176 |      0,479 |      0,096 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      4,916 |      4,076 |      6,073 |      0,700 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,414 |      0,284 |      0,727 |      0,125 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      9,112 |      5,040 |     14,292 |      3,159 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      6,403 |      4,164 |     10,222 |      1,838 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      6,276 |      3,806 |     12,427 |      2,491 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,672 |      0,242 |      3,209 |      0,873 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      8,236 |      3,548 |     23,756 |      5,743 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,195 |      0,167 |      0,286 |      0,038 |

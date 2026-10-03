# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    139,939 |     80,469 |    190,316 |     29,435 |
| UPDATE               | 10 records                                         |    190,196 |    131,080 |    279,286 |     52,041 |
| TRANSACTION          | 10 records                                         |    565,840 |    207,049 |   1420,175 |    358,624 |
| READ_UNCOMMITTED     | 10 records                                         |   3995,967 |   3338,576 |   5954,705 |    691,278 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,774 |      0,476 |      2,334 |      0,550 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,533 |      0,415 |      0,656 |      0,072 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      5,972 |      4,905 |      8,175 |      0,964 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,987 |      0,551 |      1,736 |      0,379 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      5,772 |      4,768 |      7,693 |      0,852 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      6,755 |      4,459 |     12,721 |      2,199 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      7,958 |      4,438 |     15,105 |      3,359 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,046 |      0,464 |      2,724 |      0,635 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      5,710 |      3,748 |      8,674 |      1,599 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,945 |      0,318 |      2,777 |      0,920 |

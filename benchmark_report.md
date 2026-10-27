# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     55,389 |     39,595 |     80,636 |     14,279 |
| UPDATE               | 10 records                                         |     80,811 |     63,365 |    100,199 |     10,619 |
| TRANSACTION          | 10 records                                         |    169,373 |    136,472 |    239,331 |     30,629 |
| READ_UNCOMMITTED     | 10 records                                         |    122,967 |    114,958 |    126,172 |      2,854 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,449 |      0,337 |      0,689 |      0,094 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,313 |      0,249 |      0,372 |      0,041 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,615 |      0,482 |      0,989 |      0,144 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,439 |      0,365 |      0,523 |      0,044 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,409 |      0,300 |      0,589 |      0,096 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,338 |      0,310 |      0,379 |      0,021 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,431 |      0,364 |      0,516 |      0,045 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,468 |      0,343 |      0,642 |      0,108 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,373 |      0,327 |      0,489 |      0,047 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,418 |      0,363 |      0,529 |      0,051 |

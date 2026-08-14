# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     17,963 |     14,611 |     26,434 |      3,521 |
| UPDATE               | 10 records                                         |     33,670 |     30,691 |     36,076 |      1,809 |
| TRANSACTION          | 10 records                                         |     58,429 |     50,155 |     73,927 |      6,856 |
| READ_UNCOMMITTED     | 10 records                                         |     60,501 |     56,822 |     62,355 |      1,568 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,451 |      0,342 |      1,163 |      0,238 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,347 |      0,303 |      0,520 |      0,062 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,391 |      0,263 |      0,728 |      0,160 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,535 |      0,357 |      0,899 |      0,197 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,366 |      0,340 |      0,481 |      0,039 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,427 |      0,323 |      0,515 |      0,061 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,838 |      0,742 |      0,945 |      0,068 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,866 |      0,696 |      1,304 |      0,207 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,381 |      0,331 |      0,492 |      0,046 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,637 |      0,435 |      1,050 |      0,215 |

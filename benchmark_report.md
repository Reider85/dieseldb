# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     16,306 |     13,933 |     22,699 |      2,459 |
| UPDATE               | 10 records                                         |     31,964 |     27,399 |     42,898 |      4,738 |
| TRANSACTION          | 10 records                                         |     51,575 |     44,542 |     59,826 |      6,205 |
| READ_UNCOMMITTED     | 10 records                                         |     60,812 |     56,706 |     71,423 |      3,974 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,373 |      0,272 |      0,547 |      0,103 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,269 |      0,257 |      0,288 |      0,010 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,366 |      0,262 |      0,620 |      0,132 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,412 |      0,370 |      0,615 |      0,068 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,545 |      0,415 |      1,000 |      0,170 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,465 |      0,358 |      0,750 |      0,112 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,953 |      0,766 |      1,295 |      0,186 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,180 |      0,806 |      2,494 |      0,471 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,449 |      0,412 |      0,594 |      0,052 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,649 |      0,554 |      0,868 |      0,096 |

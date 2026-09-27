# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     74,812 |     62,057 |     93,483 |      8,832 |
| UPDATE               | 10 records                                         |    148,128 |    121,517 |    188,833 |     19,333 |
| TRANSACTION          | 10 records                                         |    239,066 |    194,920 |    314,530 |     34,014 |
| READ_UNCOMMITTED     | 10 records                                         |    189,563 |    121,264 |    322,593 |     75,678 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,559 |      0,445 |      0,916 |      0,133 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,370 |      0,306 |      0,461 |      0,046 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,640 |      0,428 |      1,163 |      0,238 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,713 |      0,457 |      1,215 |      0,240 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,617 |      0,414 |      1,314 |      0,262 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,641 |      0,355 |      1,166 |      0,272 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,582 |      0,412 |      1,372 |      0,271 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,965 |      0,480 |      2,384 |      0,629 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,516 |      0,345 |      1,152 |      0,250 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,038 |      0,418 |      4,497 |      1,168 |

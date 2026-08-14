# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     16,256 |     13,576 |     20,012 |      1,896 |
| UPDATE               | 10 records                                         |     32,322 |     27,621 |     42,595 |      5,086 |
| TRANSACTION          | 10 records                                         |     51,979 |     42,598 |     62,160 |      5,414 |
| READ_UNCOMMITTED     | 10 records                                         |     60,423 |     57,728 |     61,711 |      1,096 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,291 |      0,267 |      0,371 |      0,030 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,569 |      0,244 |      2,607 |      0,693 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,901 |      0,270 |      4,715 |      1,285 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,374 |      0,348 |      0,510 |      0,048 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,697 |      0,370 |      2,631 |      0,653 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,335 |      0,299 |      0,410 |      0,034 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,758 |      0,668 |      0,992 |      0,094 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,961 |      0,799 |      1,391 |      0,175 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,424 |      0,376 |      0,500 |      0,041 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,562 |      0,474 |      0,832 |      0,096 |

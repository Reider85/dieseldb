# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     65,464 |     54,710 |     96,647 |     12,458 |
| UPDATE               | 10 records                                         |     97,825 |     84,888 |    113,001 |      8,899 |
| TRANSACTION          | 10 records                                         |    231,410 |    164,549 |    334,234 |     45,905 |
| READ_UNCOMMITTED     | 10 records                                         |    125,266 |    116,935 |    145,012 |      8,091 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,491 |      0,407 |      0,763 |      0,101 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,427 |      0,303 |      0,743 |      0,138 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,558 |      0,450 |      0,754 |      0,087 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,584 |      0,456 |      0,779 |      0,094 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,433 |      0,383 |      0,510 |      0,037 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,678 |      0,376 |      1,545 |      0,415 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,719 |      0,491 |      1,774 |      0,361 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,596 |      0,465 |      0,800 |      0,111 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,405 |      0,330 |      0,540 |      0,051 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,473 |      0,337 |      1,000 |      0,196 |

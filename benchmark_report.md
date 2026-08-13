# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     15,431 |     11,633 |     20,274 |      2,551 |
| UPDATE               | 10 records                                         |     25,023 |     22,899 |     27,216 |      1,369 |
| TRANSACTION          | 10 records                                         |     44,430 |     40,381 |     49,947 |      2,576 |
| READ_UNCOMMITTED     | 10 records                                         |     60,639 |     57,313 |     62,549 |      1,605 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,314 |      0,268 |      0,417 |      0,044 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,256 |      0,236 |      0,286 |      0,012 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,431 |      0,300 |      0,852 |      0,161 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,460 |      0,362 |      0,681 |      0,100 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,765 |      0,448 |      2,053 |      0,503 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,300 |      0,282 |      0,371 |      0,025 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,606 |      0,593 |      0,641 |      0,015 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,634 |      0,612 |      0,703 |      0,026 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,443 |      0,332 |      0,894 |      0,178 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,596 |      0,470 |      0,779 |      0,123 |

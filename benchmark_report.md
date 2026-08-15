# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     17,171 |     12,844 |     28,333 |      4,326 |
| UPDATE               | 10 records                                         |     32,967 |     26,925 |     39,945 |      4,046 |
| TRANSACTION          | 10 records                                         |     41,044 |     32,791 |     56,352 |      8,045 |
| READ_UNCOMMITTED     | 10 records                                         |     59,510 |     55,820 |     62,018 |      1,815 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,392 |      0,310 |      0,794 |      0,141 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,311 |      0,292 |      0,334 |      0,014 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,392 |      0,327 |      0,542 |      0,069 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,602 |      0,443 |      1,213 |      0,250 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,519 |      0,438 |      0,764 |      0,090 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,540 |      0,364 |      1,246 |      0,251 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,933 |      0,765 |      1,260 |      0,147 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,974 |      0,745 |      1,670 |      0,274 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,545 |      0,389 |      0,970 |      0,226 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,517 |      0,484 |      0,634 |      0,043 |

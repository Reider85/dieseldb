# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     54,200 |     47,664 |     67,290 |      6,823 |
| UPDATE               | 10 records                                         |    115,756 |     85,977 |    207,599 |     35,831 |
| TRANSACTION          | 10 records                                         |    203,374 |    124,219 |    750,399 |    183,283 |
| READ_UNCOMMITTED     | 10 records                                         |    129,119 |    111,853 |    151,122 |     10,772 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,576 |      0,380 |      0,879 |      0,156 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,532 |      0,324 |      0,872 |      0,178 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      1,962 |      0,580 |      6,521 |      1,810 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      1,353 |      0,495 |      4,055 |      1,302 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,355 |      0,288 |      0,401 |      0,034 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,612 |      0,292 |      2,884 |      0,758 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,615 |      0,392 |      1,818 |      0,411 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,485 |      0,364 |      0,689 |      0,121 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      6,449 |      0,344 |     34,285 |      9,942 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,421 |      0,273 |      0,681 |      0,139 |

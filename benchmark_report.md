# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    161,402 |     92,388 |    288,125 |     62,728 |
| UPDATE               | 10 records                                         |    148,041 |    121,015 |    186,855 |     21,904 |
| TRANSACTION          | 10 records                                         |    582,789 |    204,742 |   1163,802 |    378,081 |
| READ_UNCOMMITTED     | 10 records                                         |    120,845 |    116,248 |    127,235 |      3,467 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,308 |      0,273 |      0,405 |      0,039 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,249 |      0,206 |      0,340 |      0,037 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      4,612 |      3,525 |      7,382 |      1,195 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,320 |      0,285 |      0,403 |      0,033 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      3,867 |      3,265 |      4,947 |      0,544 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      3,710 |      2,992 |      4,325 |      0,422 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      4,865 |      4,030 |      6,100 |      0,643 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,402 |      0,305 |      0,692 |      0,108 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      4,372 |      3,135 |      6,184 |      0,928 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,405 |      0,263 |      0,705 |      0,126 |

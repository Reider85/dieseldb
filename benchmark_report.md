# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     67,416 |     46,611 |    167,239 |     34,011 |
| UPDATE               | 10 records                                         |    140,395 |    132,427 |    151,979 |      5,953 |
| TRANSACTION          | 10 records                                         |    185,159 |    154,425 |    224,716 |     21,263 |
| READ_UNCOMMITTED     | 10 records                                         |    123,289 |    118,473 |    130,012 |      2,844 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,504 |      0,373 |      0,676 |      0,110 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,312 |      0,202 |      0,577 |      0,121 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,539 |      0,391 |      0,918 |      0,156 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,508 |      0,397 |      1,025 |      0,182 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,649 |      0,328 |      2,990 |      0,781 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,517 |      0,340 |      0,819 |      0,155 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,442 |      0,378 |      0,522 |      0,049 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,441 |      0,372 |      0,522 |      0,054 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,343 |      0,290 |      0,408 |      0,028 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,390 |      0,294 |      0,500 |      0,064 |

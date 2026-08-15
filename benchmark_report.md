# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     26,723 |     17,889 |     48,146 |      9,560 |
| UPDATE               | 10 records                                         |     33,642 |     30,132 |     42,365 |      3,311 |
| TRANSACTION          | 10 records                                         |     56,816 |     47,189 |     75,789 |      8,969 |
| READ_UNCOMMITTED     | 10 records                                         |     62,752 |     56,649 |     74,800 |      4,934 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,574 |      0,408 |      0,837 |      0,177 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,393 |      0,372 |      0,444 |      0,020 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,766 |      0,432 |      1,948 |      0,444 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,746 |      0,539 |      2,108 |      0,455 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,869 |      0,504 |      1,900 |      0,424 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,503 |      0,472 |      0,564 |      0,032 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      1,350 |      0,988 |      2,543 |      0,550 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,106 |      0,937 |      1,507 |      0,160 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,675 |      0,478 |      1,453 |      0,314 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,893 |      0,605 |      1,829 |      0,379 |

# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     20,236 |     15,220 |     31,216 |      4,320 |
| UPDATE               | 10 records                                         |     35,179 |     32,037 |     41,030 |      2,810 |
| TRANSACTION          | 10 records                                         |     59,688 |     39,929 |    141,089 |     29,184 |
| READ_UNCOMMITTED     | 10 records                                         |     61,278 |     57,574 |     70,624 |      3,556 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,334 |      0,303 |      0,427 |      0,040 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,398 |      0,295 |      0,925 |      0,185 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,569 |      0,331 |      2,266 |      0,567 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,744 |      0,510 |      1,393 |      0,290 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,565 |      0,519 |      0,641 |      0,039 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,621 |      0,461 |      1,134 |      0,221 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      1,263 |      0,983 |      2,266 |      0,421 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,864 |      0,840 |      0,944 |      0,030 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,683 |      0,584 |      0,892 |      0,092 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,399 |      0,697 |      6,638 |      1,748 |

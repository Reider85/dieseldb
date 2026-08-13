# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     15,354 |     11,919 |     25,454 |      3,813 |
| UPDATE               | 10 records                                         |     31,536 |     24,072 |     41,741 |      4,927 |
| TRANSACTION          | 10 records                                         |     61,665 |     53,170 |     84,862 |      9,108 |
| READ_UNCOMMITTED     | 10 records                                         |     62,671 |     57,300 |     73,977 |      5,460 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,631 |      0,419 |      2,149 |      0,508 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,489 |      0,363 |      0,875 |      0,190 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,419 |      0,378 |      0,468 |      0,032 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,585 |      0,504 |      0,960 |      0,133 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,684 |      0,507 |      1,348 |      0,247 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,633 |      0,385 |      2,236 |      0,537 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      1,197 |      0,889 |      2,875 |      0,582 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,880 |      0,837 |      0,960 |      0,033 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,760 |      0,427 |      2,626 |      0,656 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,831 |      0,527 |      3,339 |      0,836 |

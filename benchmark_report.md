# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     48,343 |     42,929 |     53,400 |      3,112 |
| UPDATE               | 10 records                                         |     76,778 |     69,444 |     83,137 |      4,887 |
| TRANSACTION          | 10 records                                         |    185,070 |    148,295 |    265,815 |     36,802 |
| READ_UNCOMMITTED     | 10 records                                         |    125,035 |    117,196 |    137,675 |      5,504 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,720 |      0,441 |      1,078 |      0,212 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,370 |      0,278 |      0,466 |      0,056 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,600 |      0,498 |      0,680 |      0,059 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,824 |      0,459 |      2,677 |      0,645 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,455 |      0,367 |      0,598 |      0,070 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,434 |      0,356 |      0,622 |      0,082 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,445 |      0,388 |      0,526 |      0,045 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,453 |      0,406 |      0,534 |      0,039 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,353 |      0,298 |      0,417 |      0,040 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,500 |      0,314 |      0,825 |      0,161 |

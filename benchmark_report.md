# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     33,461 |     27,614 |     36,547 |      2,483 |
| UPDATE               | 10 records                                         |     55,946 |     49,872 |     60,107 |      3,087 |
| TRANSACTION          | 10 records                                         |    100,029 |     89,795 |    113,801 |      8,104 |
| READ_UNCOMMITTED     | 10 records                                         |    122,458 |    114,125 |    125,672 |      3,165 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,362 |      0,272 |      0,825 |      0,157 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,294 |      0,193 |      0,632 |      0,125 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,399 |      0,329 |      0,622 |      0,078 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,665 |      0,326 |      1,875 |      0,436 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,297 |      0,238 |      0,464 |      0,063 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,405 |      0,268 |      1,013 |      0,220 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,393 |      0,281 |      0,706 |      0,131 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,283 |      0,251 |      0,313 |      0,021 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,288 |      0,200 |      0,756 |      0,161 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,199 |      0,174 |      0,293 |      0,033 |

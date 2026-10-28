# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     64,033 |     52,904 |     85,368 |      9,246 |
| UPDATE               | 10 records                                         |    132,472 |     99,064 |    229,605 |     35,796 |
| TRANSACTION          | 10 records                                         |    259,179 |    203,199 |    482,024 |     80,348 |
| READ_UNCOMMITTED     | 10 records                                         |    126,473 |    115,997 |    133,583 |      5,962 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,227 |      0,194 |      0,297 |      0,033 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,156 |      0,134 |      0,210 |      0,023 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,298 |      0,253 |      0,387 |      0,048 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,275 |      0,235 |      0,404 |      0,048 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,261 |      0,180 |      0,447 |      0,099 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,252 |      0,192 |      0,505 |      0,089 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,558 |      0,262 |      1,935 |      0,518 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,302 |      0,231 |      0,513 |      0,083 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,210 |      0,181 |      0,246 |      0,026 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,200 |      0,160 |      0,439 |      0,080 |

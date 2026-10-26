# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     49,734 |     43,146 |     55,179 |      3,579 |
| UPDATE               | 10 records                                         |     83,431 |     70,998 |     95,126 |      6,721 |
| TRANSACTION          | 10 records                                         |    183,148 |    147,622 |    220,633 |     25,208 |
| READ_UNCOMMITTED     | 10 records                                         |    123,540 |    113,209 |    130,014 |      5,552 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,471 |      0,382 |      0,586 |      0,065 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,333 |      0,276 |      0,388 |      0,027 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,600 |      0,399 |      0,769 |      0,130 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,428 |      0,376 |      0,504 |      0,035 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,462 |      0,303 |      0,881 |      0,175 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,368 |      0,293 |      0,472 |      0,065 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,756 |      0,471 |      1,346 |      0,260 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,543 |      0,487 |      0,638 |      0,043 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,857 |      0,352 |      3,542 |      0,931 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,443 |      0,333 |      0,589 |      0,089 |

# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     83,003 |     68,029 |    104,079 |     10,172 |
| UPDATE               | 10 records                                         |    147,595 |    120,986 |    179,857 |     21,632 |
| TRANSACTION          | 10 records                                         |    310,532 |    233,453 |    503,558 |     77,270 |
| READ_UNCOMMITTED     | 10 records                                         |    144,835 |    131,431 |    170,147 |     11,346 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      1,297 |      0,603 |      5,256 |      1,335 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      1,167 |      0,518 |      3,719 |      0,995 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      2,931 |      0,616 |     10,117 |      3,378 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |     12,649 |      0,801 |     48,570 |     15,993 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      1,625 |      0,521 |      7,536 |      2,161 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      1,603 |      0,438 |      6,968 |      2,154 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      7,965 |      0,599 |     28,166 |      9,600 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      2,308 |      0,555 |      9,263 |      2,794 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      2,177 |      0,369 |     16,986 |      4,938 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,262 |      0,390 |      7,993 |      2,246 |

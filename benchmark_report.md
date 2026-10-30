# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     41,178 |     36,220 |     50,039 |      3,433 |
| UPDATE               | 10 records                                         |     72,023 |     62,286 |     83,875 |      6,144 |
| TRANSACTION          | 10 records                                         |    131,353 |    118,175 |    153,083 |      9,418 |
| READ_UNCOMMITTED     | 10 records                                         |    122,396 |    113,270 |    127,217 |      3,899 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,272 |      0,214 |      0,324 |      0,033 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,177 |      0,145 |      0,235 |      0,024 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,326 |      0,263 |      0,410 |      0,051 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,241 |      0,206 |      0,291 |      0,030 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,216 |      0,171 |      0,322 |      0,043 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,227 |      0,197 |      0,304 |      0,032 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,271 |      0,201 |      0,447 |      0,067 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,282 |      0,223 |      0,469 |      0,068 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,185 |      0,165 |      0,265 |      0,030 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,177 |      0,138 |      0,399 |      0,075 |

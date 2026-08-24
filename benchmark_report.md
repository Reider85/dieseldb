# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     16,138 |     11,725 |     21,060 |      2,729 |
| UPDATE               | 10 records                                         |     26,684 |     23,864 |     28,567 |      1,375 |
| TRANSACTION          | 10 records                                         |     50,116 |     42,658 |     58,073 |      4,549 |
| READ_UNCOMMITTED     | 10 records                                         |     62,197 |     58,613 |     71,193 |      3,394 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,062 |      0,054 |      0,087 |      0,012 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,052 |      0,048 |      0,062 |      0,005 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,070 |      0,067 |      0,081 |      0,004 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,075 |      0,062 |      0,104 |      0,014 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,110 |      0,064 |      0,379 |      0,092 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,078 |      0,060 |      0,129 |      0,020 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,072 |      0,060 |      0,087 |      0,008 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,160 |      0,085 |      0,280 |      0,062 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,068 |      0,062 |      0,091 |      0,009 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,247 |      0,061 |      1,527 |      0,434 |

# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     16,645 |     12,488 |     20,104 |      2,658 |
| UPDATE               | 10 records                                         |     29,320 |     25,428 |     37,176 |      3,266 |
| TRANSACTION          | 10 records                                         |     56,043 |     47,110 |     59,687 |      3,553 |
| READ_UNCOMMITTED     | 10 records                                         |     66,725 |     58,773 |     90,627 |      9,471 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,105 |      0,069 |      0,181 |      0,043 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,075 |      0,066 |      0,112 |      0,015 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,108 |      0,089 |      0,168 |      0,024 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,107 |      0,094 |      0,146 |      0,016 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,091 |      0,078 |      0,133 |      0,016 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,078 |      0,073 |      0,092 |      0,007 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,092 |      0,082 |      0,117 |      0,013 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,100 |      0,091 |      0,124 |      0,010 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,081 |      0,075 |      0,106 |      0,010 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,080 |      0,073 |      0,096 |      0,008 |

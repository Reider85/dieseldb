# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     24,799 |     19,121 |     41,159 |      5,947 |
| UPDATE               | 10 records                                         |     47,912 |     35,055 |     61,453 |      8,538 |
| TRANSACTION          | 10 records                                         |     74,994 |     47,194 |    199,584 |     44,882 |
| READ_UNCOMMITTED     | 10 records                                         |     79,154 |     58,438 |    129,645 |     21,293 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,200 |      0,082 |      0,669 |      0,164 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,087 |      0,080 |      0,114 |      0,010 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,092 |      0,085 |      0,118 |      0,010 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,115 |      0,089 |      0,264 |      0,051 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,146 |      0,085 |      0,362 |      0,086 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,083 |      0,080 |      0,096 |      0,005 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,096 |      0,092 |      0,118 |      0,008 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,163 |      0,095 |      0,512 |      0,125 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,089 |      0,077 |      0,137 |      0,017 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,089 |      0,081 |      0,108 |      0,008 |

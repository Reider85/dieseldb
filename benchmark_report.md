# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    183,148 |    145,728 |    225,848 |     27,030 |
| UPDATE               | 10 records                                         |    518,019 |    354,771 |    773,449 |    119,953 |
| TRANSACTION          | 10 records                                         |    777,132 |    507,553 |    970,227 |    124,580 |
| READ_UNCOMMITTED     | 10 records                                         |    261,109 |    142,002 |    885,089 |    211,268 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,926 |      0,499 |      2,338 |      0,533 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      1,856 |      0,282 |      6,713 |      1,992 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |     28,805 |     19,625 |     48,993 |      9,099 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      5,094 |      0,594 |     18,314 |      6,657 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |     61,591 |     16,891 |    200,677 |     53,265 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |     41,357 |     11,116 |     69,909 |     18,471 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |     22,809 |     10,486 |     48,067 |     12,413 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      3,759 |      0,893 |     10,754 |      3,165 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |     35,861 |     11,680 |    143,913 |     37,195 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |     16,746 |      0,556 |     69,608 |     21,908 |

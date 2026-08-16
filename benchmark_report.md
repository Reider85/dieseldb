# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     24,109 |     17,251 |     34,074 |      4,548 |
| UPDATE               | 10 records                                         |     52,731 |     40,518 |     68,382 |      8,933 |
| TRANSACTION          | 10 records                                         |     79,376 |     60,909 |    134,988 |     20,417 |
| READ_UNCOMMITTED     | 10 records                                         |     65,049 |     57,804 |     73,703 |      6,067 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,132 |      0,061 |      0,304 |      0,087 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,077 |      0,062 |      0,127 |      0,024 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,079 |      0,065 |      0,174 |      0,032 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,077 |      0,071 |      0,093 |      0,007 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,098 |      0,067 |      0,183 |      0,033 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,071 |      0,064 |      0,092 |      0,010 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,101 |      0,072 |      0,228 |      0,048 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,136 |      0,074 |      0,520 |      0,129 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,071 |      0,062 |      0,091 |      0,010 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,089 |      0,065 |      0,256 |      0,056 |

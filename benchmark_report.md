# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     18,434 |     13,332 |     29,174 |      4,580 |
| UPDATE               | 10 records                                         |     35,018 |     28,838 |     52,863 |      7,147 |
| TRANSACTION          | 10 records                                         |     57,991 |     48,945 |     78,272 |      8,588 |
| READ_UNCOMMITTED     | 10 records                                         |     60,507 |     58,716 |     62,393 |      0,965 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,048 |      0,043 |      0,065 |      0,008 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,044 |      0,041 |      0,057 |      0,005 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,081 |      0,054 |      0,224 |      0,049 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,118 |      0,078 |      0,359 |      0,083 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,065 |      0,060 |      0,079 |      0,007 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,057 |      0,055 |      0,072 |      0,005 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,071 |      0,064 |      0,090 |      0,009 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,113 |      0,072 |      0,227 |      0,050 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,068 |      0,060 |      0,093 |      0,011 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,064 |      0,056 |      0,078 |      0,008 |

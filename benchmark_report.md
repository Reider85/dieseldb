# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    116,715 |    106,785 |    131,382 |      6,922 |
| UPDATE               | 10 records                                         |    245,255 |    222,091 |    275,462 |     16,875 |
| TRANSACTION          | 10 records                                         |    359,181 |    341,494 |    393,616 |     14,295 |
| READ_UNCOMMITTED     | 10 records                                         |     68,190 |     60,970 |     73,926 |      4,933 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,046 |      0,041 |      0,070 |      0,009 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,041 |      0,038 |      0,050 |      0,004 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,064 |      0,053 |      0,114 |      0,017 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,065 |      0,057 |      0,099 |      0,012 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,066 |      0,047 |      0,165 |      0,037 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,082 |      0,044 |      0,186 |      0,051 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,063 |      0,057 |      0,093 |      0,011 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,078 |      0,064 |      0,105 |      0,015 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,056 |      0,048 |      0,109 |      0,018 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,058 |      0,050 |      0,084 |      0,011 |

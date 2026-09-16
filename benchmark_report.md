# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    242,718 |    110,069 |    440,957 |    124,740 |
| UPDATE               | 10 records                                         |    275,757 |    201,604 |    389,968 |     54,378 |
| TRANSACTION          | 10 records                                         |    330,569 |    289,131 |    466,059 |     47,436 |
| READ_UNCOMMITTED     | 10 records                                         |    123,327 |    116,785 |    126,919 |      2,931 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,093 |      0,080 |      0,158 |      0,024 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,090 |      0,070 |      0,165 |      0,028 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,102 |      0,089 |      0,124 |      0,013 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,125 |      0,096 |      0,207 |      0,034 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,092 |      0,080 |      0,127 |      0,015 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,086 |      0,071 |      0,189 |      0,034 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,111 |      0,089 |      0,198 |      0,031 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,111 |      0,095 |      0,140 |      0,015 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,125 |      0,076 |      0,303 |      0,066 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,085 |      0,078 |      0,102 |      0,008 |

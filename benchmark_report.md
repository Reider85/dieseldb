# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     30,544 |     14,330 |     85,472 |     20,027 |
| UPDATE               | 10 records                                         |     40,706 |     27,475 |     69,697 |     12,792 |
| TRANSACTION          | 10 records                                         |     92,850 |     65,862 |    147,469 |     24,507 |
| READ_UNCOMMITTED     | 10 records                                         |     75,359 |     60,152 |    119,406 |     15,274 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,121 |      0,076 |      0,306 |      0,071 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,075 |      0,067 |      0,099 |      0,011 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,128 |      0,086 |      0,284 |      0,058 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,109 |      0,096 |      0,145 |      0,018 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,090 |      0,079 |      0,112 |      0,012 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,080 |      0,073 |      0,096 |      0,006 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,089 |      0,082 |      0,122 |      0,011 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,107 |      0,094 |      0,179 |      0,025 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,087 |      0,076 |      0,122 |      0,015 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,079 |      0,072 |      0,102 |      0,010 |

# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     15,284 |     12,370 |     19,023 |      1,722 |
| UPDATE               | 10 records                                         |     34,597 |     30,586 |     39,624 |      2,501 |
| TRANSACTION          | 10 records                                         |     51,358 |     46,640 |     57,091 |      3,232 |
| READ_UNCOMMITTED     | 10 records                                         |     60,310 |     57,518 |     69,809 |      3,311 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,062 |      0,056 |      0,100 |      0,013 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,067 |      0,056 |      0,129 |      0,021 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,066 |      0,060 |      0,082 |      0,007 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,073 |      0,065 |      0,111 |      0,013 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,092 |      0,063 |      0,175 |      0,041 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,071 |      0,060 |      0,113 |      0,017 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,077 |      0,069 |      0,129 |      0,018 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,131 |      0,078 |      0,236 |      0,056 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,168 |      0,060 |      0,700 |      0,201 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,180 |      0,065 |      0,563 |      0,165 |

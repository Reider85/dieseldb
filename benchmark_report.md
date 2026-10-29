# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     40,590 |     34,068 |     46,114 |      4,349 |
| UPDATE               | 10 records                                         |     62,838 |     55,738 |     68,583 |      4,056 |
| TRANSACTION          | 10 records                                         |    135,483 |    123,776 |    164,471 |     12,929 |
| READ_UNCOMMITTED     | 10 records                                         |    122,287 |    116,191 |    128,744 |      3,577 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,210 |      0,181 |      0,322 |      0,041 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,203 |      0,132 |      0,363 |      0,077 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,495 |      0,258 |      1,468 |      0,372 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,249 |      0,206 |      0,359 |      0,047 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,251 |      0,183 |      0,573 |      0,109 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,363 |      0,193 |      0,621 |      0,164 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,260 |      0,201 |      0,405 |      0,054 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,381 |      0,227 |      0,844 |      0,195 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,201 |      0,169 |      0,383 |      0,062 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,208 |      0,142 |      0,482 |      0,097 |

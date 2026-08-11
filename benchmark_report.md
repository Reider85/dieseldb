# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     31,511 |     18,830 |     46,285 |      8,198 |
| UPDATE               | 10 records                                         |     62,717 |     43,171 |     92,526 |     17,369 |
| TRANSACTION          | 10 records                                         |     80,662 |     66,278 |    116,708 |     15,501 |
| READ_UNCOMMITTED     | 10 records                                         |     65,908 |     57,953 |    101,546 |     12,670 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      1,126 |      0,943 |      1,512 |      0,183 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,960 |      0,846 |      1,182 |      0,111 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      1,034 |      0,820 |      1,481 |      0,174 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      1,253 |      1,150 |      1,474 |      0,095 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      1,293 |      0,998 |      1,910 |      0,290 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      1,273 |      0,976 |      2,015 |      0,309 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      2,982 |      1,980 |      6,855 |      1,511 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      2,494 |      1,800 |      5,494 |      1,063 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,045 |      0,863 |      1,672 |      0,230 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,403 |      1,102 |      2,185 |      0,298 |

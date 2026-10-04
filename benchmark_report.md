# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     78,848 |     67,057 |    111,659 |     12,851 |
| UPDATE               | 10 records                                         |     79,817 |     75,691 |     84,606 |      2,655 |
| TRANSACTION          | 10 records                                         |    188,874 |    165,373 |    215,661 |     16,315 |
| READ_UNCOMMITTED     | 10 records                                         |    144,756 |    117,683 |    345,358 |     66,909 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,217 |      0,188 |      0,269 |      0,028 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,206 |      0,148 |      0,445 |      0,088 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      3,038 |      2,342 |      3,582 |      0,402 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,230 |      0,196 |      0,271 |      0,021 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      3,208 |      2,655 |      3,842 |      0,327 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      2,809 |      2,440 |      3,274 |      0,295 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      2,893 |      2,379 |      3,816 |      0,455 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,305 |      0,170 |      1,089 |      0,266 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      2,788 |      2,323 |      4,211 |      0,519 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,509 |      0,119 |      3,669 |      1,055 |

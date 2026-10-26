# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     57,709 |     50,509 |     78,498 |      8,711 |
| UPDATE               | 10 records                                         |     95,505 |     70,519 |    123,192 |     13,011 |
| TRANSACTION          | 10 records                                         |    180,746 |    150,979 |    226,667 |     23,633 |
| READ_UNCOMMITTED     | 10 records                                         |    121,736 |    116,743 |    129,000 |      3,553 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,477 |      0,363 |      0,726 |      0,103 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,366 |      0,317 |      0,466 |      0,038 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,640 |      0,489 |      1,265 |      0,214 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,768 |      0,503 |      1,133 |      0,212 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,445 |      0,296 |      0,670 |      0,123 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      1,060 |      0,380 |      3,327 |      0,858 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,961 |      0,385 |      3,862 |      1,053 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,885 |      0,397 |      2,391 |      0,641 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,442 |      0,324 |      0,780 |      0,142 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,303 |      0,260 |      0,424 |      0,047 |

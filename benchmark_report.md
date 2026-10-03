# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    199,177 |    131,518 |    302,350 |     55,342 |
| UPDATE               | 10 records                                         |    176,915 |    113,961 |    226,763 |     36,759 |
| TRANSACTION          | 10 records                                         |    408,815 |    299,038 |    562,487 |     69,296 |
| READ_UNCOMMITTED     | 10 records                                         |   3834,961 |   3342,281 |   5107,202 |    558,865 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,440 |      0,335 |      0,606 |      0,074 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,420 |      0,331 |      0,489 |      0,046 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      6,125 |      4,455 |      8,920 |      1,706 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      1,302 |      0,514 |      5,075 |      1,344 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      7,035 |      4,767 |     10,440 |      1,787 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      6,305 |      4,048 |     10,353 |      2,055 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |     10,342 |      4,391 |     27,184 |      7,328 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,624 |      0,393 |      1,875 |      0,423 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |     10,984 |      4,277 |     21,539 |      5,828 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,818 |      0,456 |      1,304 |      0,291 |

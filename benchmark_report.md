# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     45,513 |     28,964 |     67,723 |     14,425 |
| UPDATE               | 10 records                                         |     57,531 |     38,357 |     83,823 |     13,219 |
| TRANSACTION          | 10 records                                         |     58,955 |     49,633 |     71,010 |      6,290 |
| READ_UNCOMMITTED     | 10 records                                         |     71,273 |     59,176 |     82,178 |      7,717 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      2,663 |      0,577 |     19,355 |      5,571 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      1,748 |      0,642 |      7,631 |      2,011 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      1,982 |      0,570 |     12,285 |      3,442 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      1,205 |      0,835 |      2,966 |      0,613 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      1,032 |      0,746 |      2,009 |      0,378 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      1,420 |      0,612 |      6,189 |      1,654 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      1,983 |      1,267 |      3,520 |      0,760 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      4,126 |      1,305 |     11,453 |      3,473 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,824 |      0,639 |      1,243 |      0,180 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,444 |      0,825 |      2,585 |      0,643 |

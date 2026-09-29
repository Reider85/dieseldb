# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     61,296 |     53,092 |     75,031 |      6,177 |
| UPDATE               | 10 records                                         |    157,223 |    116,260 |    272,607 |     44,420 |
| TRANSACTION          | 10 records                                         |    291,236 |    174,561 |    580,711 |    113,340 |
| READ_UNCOMMITTED     | 10 records                                         |    186,821 |    140,034 |    369,266 |     64,786 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      1,625 |      0,436 |      5,826 |      2,063 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      1,782 |      0,287 |      7,478 |      2,630 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      2,150 |      0,415 |      7,620 |      2,528 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      1,426 |      0,415 |      5,582 |      1,863 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      1,598 |      0,368 |      7,179 |      2,410 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      2,733 |      0,321 |      7,504 |      3,019 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      1,333 |      0,329 |      5,007 |      1,754 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,104 |      0,322 |      6,588 |      1,844 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      1,046 |      0,305 |      5,385 |      1,479 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      2,694 |      0,272 |      8,646 |      3,446 |

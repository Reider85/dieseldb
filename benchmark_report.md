# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     56,032 |     50,326 |     67,150 |      4,753 |
| UPDATE               | 10 records                                         |     94,569 |     86,013 |    116,510 |     10,216 |
| TRANSACTION          | 10 records                                         |    198,589 |    174,823 |    250,569 |     19,177 |
| READ_UNCOMMITTED     | 10 records                                         |    171,847 |    150,666 |    193,689 |     14,398 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      1,125 |      0,640 |      2,482 |      0,584 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,523 |      0,394 |      0,677 |      0,079 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      2,002 |      0,604 |     13,510 |      3,837 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,697 |      0,582 |      0,857 |      0,095 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,585 |      0,410 |      0,918 |      0,140 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      4,062 |      0,449 |     35,533 |     10,491 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      1,454 |      0,417 |      9,529 |      2,694 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,602 |      0,451 |      6,803 |      2,112 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,503 |      0,319 |      1,665 |      0,390 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,576 |      0,394 |      1,278 |      0,248 |

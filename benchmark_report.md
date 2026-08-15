# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     18,721 |     14,116 |     24,903 |      3,862 |
| UPDATE               | 10 records                                         |     23,519 |     18,947 |     26,630 |      2,234 |
| TRANSACTION          | 10 records                                         |     40,501 |     38,297 |     43,798 |      1,831 |
| READ_UNCOMMITTED     | 10 records                                         |     60,892 |     57,960 |     71,302 |      3,703 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,525 |      0,301 |      1,872 |      0,453 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,400 |      0,309 |      0,638 |      0,104 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,400 |      0,309 |      0,532 |      0,072 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,512 |      0,426 |      0,753 |      0,088 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,515 |      0,424 |      0,902 |      0,134 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,347 |      0,312 |      0,427 |      0,028 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,790 |      0,681 |      1,091 |      0,112 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,864 |      0,648 |      1,814 |      0,344 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,418 |      0,366 |      0,609 |      0,069 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,673 |      0,483 |      1,148 |      0,230 |

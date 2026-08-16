# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     23,796 |     14,791 |     40,340 |      8,889 |
| UPDATE               | 10 records                                         |     38,119 |     31,449 |     51,911 |      5,816 |
| TRANSACTION          | 10 records                                         |     65,523 |     55,428 |     94,529 |     10,816 |
| READ_UNCOMMITTED     | 10 records                                         |     63,896 |     57,599 |     73,523 |      5,220 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,085 |      0,076 |      0,127 |      0,014 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,084 |      0,077 |      0,104 |      0,009 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,196 |      0,080 |      0,767 |      0,197 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,342 |      0,088 |      2,436 |      0,699 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,121 |      0,085 |      0,250 |      0,053 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,090 |      0,078 |      0,152 |      0,021 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,124 |      0,090 |      0,271 |      0,051 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,114 |      0,083 |      0,329 |      0,072 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,076 |      0,069 |      0,094 |      0,008 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,145 |      0,076 |      0,386 |      0,096 |

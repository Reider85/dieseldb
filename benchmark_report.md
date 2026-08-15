# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     17,149 |     12,491 |     23,312 |      2,804 |
| UPDATE               | 10 records                                         |     29,533 |     27,298 |     34,822 |      2,109 |
| TRANSACTION          | 10 records                                         |     79,686 |     41,379 |    341,016 |     87,325 |
| READ_UNCOMMITTED     | 10 records                                         |     61,256 |     56,086 |     71,140 |      3,764 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,327 |      0,309 |      0,352 |      0,014 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,299 |      0,285 |      0,323 |      0,013 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,486 |      0,321 |      0,902 |      0,189 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,492 |      0,425 |      0,786 |      0,103 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,650 |      0,422 |      1,258 |      0,275 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,558 |      0,341 |      0,782 |      0,161 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,961 |      0,809 |      1,603 |      0,230 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,111 |      0,749 |      2,380 |      0,489 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,497 |      0,414 |      0,804 |      0,117 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,643 |      0,502 |      0,946 |      0,135 |

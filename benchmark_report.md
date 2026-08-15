# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     23,818 |     15,555 |     29,812 |      4,322 |
| UPDATE               | 10 records                                         |     40,794 |     26,598 |     82,601 |     15,625 |
| TRANSACTION          | 10 records                                         |     43,857 |     37,577 |     57,182 |      5,096 |
| READ_UNCOMMITTED     | 10 records                                         |     67,370 |     57,782 |     78,263 |      6,187 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,592 |      0,434 |      0,903 |      0,167 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,502 |      0,413 |      0,771 |      0,108 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,615 |      0,454 |      0,955 |      0,163 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      1,185 |      0,741 |      2,873 |      0,611 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      1,528 |      0,682 |      5,627 |      1,464 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,659 |      0,452 |      1,132 |      0,198 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      2,614 |      1,161 |      6,282 |      1,703 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      1,677 |      1,061 |      3,250 |      0,646 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,727 |      0,478 |      0,888 |      0,136 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,856 |      0,604 |      2,624 |      0,591 |

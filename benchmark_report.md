# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |    182,922 |    164,674 |    206,483 |     14,340 |
| UPDATE               | 10 records                                         |    656,897 |    221,691 |   1703,643 |    431,267 |
| TRANSACTION          | 10 records                                         |    987,069 |    549,959 |   1971,487 |    436,411 |
| READ_UNCOMMITTED     | 10 records                                         |    193,113 |    128,412 |    279,684 |     41,347 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |     14,276 |      0,766 |     36,078 |     14,365 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      9,490 |      0,712 |     27,286 |      9,540 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |    180,393 |     61,859 |    482,182 |    120,962 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      2,715 |      0,485 |     14,813 |      4,105 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |     39,973 |      8,418 |    132,434 |     34,177 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |     38,865 |      5,562 |     89,164 |     29,543 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |     66,413 |      4,439 |    360,720 |    101,618 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      6,347 |      0,583 |     26,747 |      7,947 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |     23,732 |      5,377 |     57,426 |     17,202 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      5,472 |      0,278 |     37,833 |     11,118 |

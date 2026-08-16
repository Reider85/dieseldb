# DieselDB Benchmark Report

| Operation            | Details                                      |   Avg (ms) |   Min (ms) |   Max (ms) | StdDev (ms) |
|----------------------|----------------------------------------------|------------|------------|------------|-------------|
| INSERT               | 10 records                                         |     22,123 |     11,325 |     40,502 |      7,923 |
| UPDATE               | 10 records                                         |     29,185 |     25,024 |     36,211 |      4,020 |
| TRANSACTION          | 10 records                                         |     63,589 |     40,845 |     83,627 |     14,844 |
| READ_UNCOMMITTED     | 10 records                                         |     61,904 |     56,302 |     72,356 |      4,437 |
| TRUE_CONDITION       | SELECT NAME, AGE FROM USERS WHERE ACTIVE = TRUE    |      0,147 |      0,064 |      0,351 |      0,105 |
| SELECT               | SELECT NAME, AGE, ACTIVE FROM USERS WHERE AGE = .. |      0,077 |      0,065 |      0,164 |      0,029 |
| SELECT               | SELECT NAME, AGE, SCORE FROM USERS WHERE SCORE >.. |      0,075 |      0,067 |      0,103 |      0,011 |
| SELECT               | SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE <.. |      0,077 |      0,074 |      0,094 |      0,006 |
| SELECT               | SELECT NAME, AGE, LEVEL FROM USERS WHERE AGE > 4.. |      0,078 |      0,069 |      0,107 |      0,012 |
| SELECT               | SELECT NAME, AGE, RANK FROM USERS WHERE NOT AGE .. |      0,109 |      0,067 |      0,368 |      0,089 |
| SELECT               | SELECT NAME, AGE, PRECISION FROM USERS WHERE (AG.. |      0,184 |      0,076 |      0,575 |      0,155 |
| SELECT               | SELECT NAME, AGE, INITIAL FROM USERS WHERE (AGE .. |      0,136 |      0,080 |      0,267 |      0,068 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,069 |      0,066 |      0,083 |      0,005 |
| SELECT               | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'C.. |      0,073 |      0,068 |      0,088 |      0,006 |

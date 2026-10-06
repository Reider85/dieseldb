# DieselDB performance regression baseline (tracked)
Generated: 2026-10-06T19:57:36.4099425

# Format: group | test | baseline_ms | query

AdvancedTest | simple select by primary key | 0.32 | SELECT ID, NAME FROM USERS WHERE ID = 50
AliasesTest | complex select min max avg with join and group by | 12.25 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC
GroupByTest | simple group by sum count | 3.86 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME
GroupByTest | complex group by join string date | 5.21 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC
InTest | simple in on btree index | 0.55 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52)
JoinTest | simple inner join on primary key | 3.10 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (50, 51, 52)
LikeTest | simple like on name | 2.98 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er50' AND NAME LIKE '%User50%' AND NAME LIKE 'User50%'
OrderByTest | simple order by age desc | 1.44 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC
PerformanceTest | simple select clustered index | 0.67 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50'
SubqueriesTest | complex subquery in column where group by having | 50.92 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10

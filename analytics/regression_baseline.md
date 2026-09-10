# DieselDB performance regression baseline (tracked)

Source: timing/timing29.md (100-row run, Prompt 17 split classes).
The PerformanceRegressionTest measures these key queries and fails the build
if any query with baseline >= 11 ms degrades by more than 20%.

# Format: group | test | baseline_ms | query

AdvancedTest | simple select by primary key | 12.34 | SELECT ID, NAME FROM USERS WHERE ID = 50
AliasesTest | complex select min max avg with join and group by | 59.99 | SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC
GroupByTest | simple group by sum count | 5.28 | SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME
GroupByTest | complex group by join string date | 37.80 | SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC
InTest | simple in on btree index | 9.97 | SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52)
JoinTest | simple inner join on primary key | 32.65 | SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (50, 51, 52)
LikeTest | simple like on name | 35.73 | SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er50' AND NAME LIKE '%User50%' AND NAME LIKE 'User50%'
OrderByTest | simple order by age desc | 4.53 | SELECT ID, AGE FROM USERS ORDER BY AGE DESC
PerformanceTest | simple select clustered index | 11.51 | SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50'
SubqueriesTest | complex subquery in column where group by having | 250.66 | SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10
DieselDB Agent Instructions (Qwen Coder)
? Repository Information
Repository: https://github.com/Reider85/dieseldb
Target Branch: develop ONLY (never main)
Language: Java 17+
Build Tool: Maven 3.9+
CI/CD: GitHub Actions (.github/workflows/ci.yml)
? Workflow Protocol
Test Execution Rules
After every code change:
| Change Scope|Command Used|When to Apply|
| ---|---|---|
| Small/Fixes|mvn test|Minor changes, bug fixes|
| Large/New Feature|mvn -Ptest test|New features, major refactoring|
Execution Steps:
git checkout develop              # Check out develop branch
mvn test                          # quick tests (default profile)
OR for large changes:
mvn -Ptest test                   # full suite (all test classes)
diff timing.md timing_new.md      # Compare timings with baseline
Performance Degradation Handling
Threshold: 20% maximum degradation allowed from baseline
IF new_time > old_time * 1.20 THEN
? STOP execution
? Identify slow queries from timing report
? Analyze query complexity increase
? Review index usage effectiveness
? Check for memory leaks or inefficiencies
? Optimize code path
? Re-test until within threshold
? Document optimization approach in techdolg.md
ENDIF
Baseline Reference Points:
Primary baseline: timing.md (latest successful run)
Historical runs: timing1.md, timing10.md, etc.
? Technical Debt Management
Assess after EVERY action completion
Debt Types to Monitor:
Type                  Description                                Example
TODO Comments         Temporary implementation notes             // TODO: optimize later
Known Limitations     Features not fully implemented             Partial SQL support
Workarounds           Quick fixes pending proper solution        Hardcoded values for testing
Unoptimized Paths     Intentionally unoptimized code             Prototype logic for performance
Missing Tests         Gaps in test coverage                      Edge cases without assertions
TechDolg Entry Format:
[Version].[Number]: [Action Description]
Date: YYYY-MM-DD
Debt Category: [Bug/Performance/Documentation/Architecture]
Impact Level: [High/Medium/Low/Critical]
Description: Clear problem statement with context
Root Cause: Why this debt was introduced
Recommended Fix: What needs to be done to resolve
Priority: [Critical/Major/Minor/P2]
Related Files: src/**/*.java, Changelog.md
Timeline: Expected resolution date or milestone
? Version Control & Changelog
Successful Action Sequence (MANDATORY ORDER):
? All tests pass (BUILD SUCCESS)
? Performance within 20% baseline documented
? Technical debt assessed ? recorded in techdolg.md if any
? Update Changelog.md with new version number
? Commit message matches changelog entry EXACTLY
? Push to develop branch only (NEVER main)
Changelog Format (Changelog.md):
[x.x.n] [Short Feature/Bug Description] - YYYY-MM-DD
Implementation Details:
Implemented: what was added
Fixed: what was corrected
Changed: what was modified
Removed: what was deprecated
Testing:
Updated: [test files]
Added: [new test classes]
Coverage impact: [% before / % after]
Performance:
Baseline comparison: timing.md
Degradation: [X]% (within acceptable limits)
Optimization note: [if applicable]
Fixes:
Bug ID references (if tracked)
Regression prevention added
Commit Message Pattern:
x.x.n: [description matching changelog exactly]
Implementation details...
?? Files NOT to Commit/Push
Automated Ignore List (Already in .gitignore):
Pattern                   Reason                        Examples
target/                   Maven build artifacts         classes/, generated-sources/
*.class                   Compiled Java bytecode        Database.class
*/surefire-reports/      JUnit test reports            TEST-AllTestsSampleTest.xml
hs_err_pid.log           JVM crash dumps               hs_err_pid1234.log
*.csv                     Test data files               USERS.csv, USER_DETAILS.csv
*.table                   Database table files          PERSISTENCE.table
*.log                     Application logs              Server logs
*.out                     Output captures               Console output dumps
DO NOT commit/push these files:
Build Artifacts:
/target/**/*
~/.m2/repository/**   # Local maven cache
pom.xml.*            # Backup pom files
Generated Reports:
target/surefire-reports/**/*
target/checkstyle-*  # Style checks
target/site/**       # Generated documentation
Temporary Files:
*.swp                # Vim swap files
*.tmp                # Temp files
.vscode/**/*         # IDE workspace files
.idea/**/*           # IntelliJ temporary files
*.iml                # Project module files
//cache/**/*       # Build caches
DO commit/push these files:
Source Code:
src/main/java/**/*.java
src/test/java/**/*.java
diesel/**/*.java     # If separate source dir
Configuration:
pom.xml
config.properties
PROFILES.csv
.github/workflows/*.yml
Documentation (tracked):
README.md
PERSISTENCE_README.md
CHANGELOG.md         # Always update on success
PROMPT.md
STEP.md
Test Results (tracked):
timing.md             # Current baseline
timing[0-9]*.md       # Historical runs
techdolg.md           # Debt tracking
testfail.md           # Failed test records
? Git Operations Checklist
Pre-Push Checklist:
? Current branch is 'develop': git branch shows develop
? Clean working tree: git status shows nothing staged that shouldn't be
? All tests passed locally: mvn test AND/OR mvn -Ptest show BUILD SUCCESS
? Performance baseline respected: ?20% degradation documented or explained
? Changelog.md updated with current version number and exact description
? Commit message follows pattern: "[version] [description]" exactly
? No unintended files staged: git status checked thoroughly
? Target directory clean: no compiled files visible in git status
? Token available for authentication: remote origin configured correctly
? Pull latest changes: git pull origin develop first
Post-Push Verification:
? GitHub UI shows successful push to develop branch
? CI pipeline triggered successfully
? No merge conflicts reported
? All automated checks passing
? Tagged version exists if release required
Push command template:
git checkout develop
git pull origin develop
git add -u
git commit -m "x.x.n: [exact description matching changelog]"
git push origin develop
? Token Configuration
Authentication Setup:
The authentication token will be provided via separate instruction/command. Configure before pushes:
Option 1: Inline token (temporary setup)
git remote set-url origin https://<TOKEN>@github.com/Reider85/dieseldb.git
Option 2: Credential helper (recommended for production)
git config --global credential.helper store
git config --global credential.helper cache
Verify configuration:
git remote -v                     # Should show correct origin
git ls-remote origin develop      # Can reach repository
Security Note: Never hardcode tokens in scripts, commit them, or expose in logs. Use environment variables or secure vault systems when possible.
?? Debugging Toolkit
Quick Diagnosis Commands:
Compilation checks:
mvn compile                        # Standard compilation
mvn clean compile                 # Fresh compilation
mvn compile -X                    # Verbose debug output
Targeted test execution:
mvn test -Dtest=AllTestsSampleTest
mvn test -Dtest=QuantitativeTest#specificMethod
mvn test -Dtest="Transaction"   # All transaction tests
Profile individual query execution:
mvn test -Dtest=PerformanceTest#queryMethodName
Memory diagnostics:
jcmd <pid> VM.native_memory summary      # Native memory analysis
jstack <pid> > thread_dump.txt           # Thread dump for hangs
jmap -heap <pid>                          # Heap analysis
Logging During Development:
// Temporary debug logging pattern (remove before commit)
private static final Logger logger = LoggerFactory.getLogger(YourClass.class);
public void debugQuery(String sql) {
logger.debug("=== DEBUG QUERY START ===");
logger.debug("SQL: {}", sql);
logger.debug("Tokens processed: {}", tokenize(sql).size());
logger.debug("Conditions parsed: {}", parseConditions(sql));
logger.debug("=== DEBUG QUERY END ===");
}
Bug Report Structure Template:
Field               Example                                    Required
ID                  BUG-001                                    Yes
Title               JOIN  OR  0                Yes
Component           QueryParser / JoinExecutor                 Yes
Steps to Reproduce  1. CREATE TABLE...\n2. INSERT rows\n3...    Yes
Expected Result     3 rows returned                            Yes
Actual Result       0 rows returned                            Yes
Severity            High (blocks feature)                      Yes
Environment         JDK 17, Maven 3.9+                         Yes
Status              Open/Fixed                                 Yes
Notes               Related to ON condition parsing            Optional
? Automate Common Tasks
Pre-Work Checklist:
? Checkout develop: git checkout develop
? Pull latest: git pull origin develop
? Clean build: mvn clean compile
? Run quick tests: mvn test
? Review timing.md: check last baseline timestamp
? Verify token availability: git remote -v
? Check disk space: df -h (~500MB minimum free)
Post-Work Checklist:
? All tests pass? Yes/No (critical blocker)
? Timing within bounds? Yes/No (>20%?)
? Changelog updated? Yes/No (blocking for commit)
? Tech debt documented? Yes/No (if applicable)
? Commit message correct? Yes/No (must match changelog)
? No target/ or .class files staged? Yes/No
? Push completed? Yes/No (blocking)
Helper Script (scripts/agent-helper.sh):
#!/bin/bash
set -e
echo "=== DieselDB Agent Workflow ==="
echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
git pull origin develop
mvn clean compile
if [[ "$1" == "--full" ]]; then
echo "Running FULL test suite..."
mvn -Ptest test
else
echo "Running QUICK tests..."
mvn test
fi
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
cp timing.md timing_${TIMESTAMP}.md || true
if [ -f "timing_baseline.md" ]; then
diff -u timing_baseline.md timing_${TIMESTAMP}.md > timing_comparison.diff || true
fi
exit $?
? Emergency Procedures
If CI/Test Suite Fails:
ACTION SEQUENCE:
Read error message in console carefully
Search testfail.md for similar patterns
Reproduce issue locally with minimal case file
Create regression test that FAILS before fix
Apply fix ? verify test passes
Update changelog entry with "Fixes: Issue #"
Push fix commit FIRST, feature commit SECOND
Rollback if needed:
git revert HEAD --no-edit
git push origin develop
Notify team immediately via channel
If Tests Are Flaky (Unstable):
Flaky Test Protocol:
Retest 3x sequentially (same conditions)
If fails 2+ times ? flag as flaky IMMEDIATELY
Add @Ignore annotation temporarily
Log issue in techdolg.md with category "flaky-test"
Set priority: P2 (medium) in tracking
NEVER ignore flaky tests without formal tracking
If Performance Degrades > 20%:
defence_protocol:
Identify slow queries from timing.md by sorting columns
Isolate problematic section (JOINs? Subqueries? Indexes?)
Analyze execution plan if available
Try optimization strategies:
Index creation for filtered columns
Query restructuring for fewer operations
Early termination conditions where safe
Reduce data loading scope
Document optimization approach and reasoning
Re-test with identical dataset conditions
If still failing ? rollback change temporarily
Escalate to senior review if >40% degradation
? Status Reporting Template
Before Each Completion Message:
? Action Completed Successfully
Version: x.x.n
Time: 2026-08-07 HH:MM
Branch: develop
Tests Passed: AllTestsSampleTest X/X + QuantitativeTest X/X
Timing Baseline: Within 20% (current: +X%)
Technical Debt: None OR documented entries
Changes Made:
Feature 1 description with technical detail
Bug fix reference [BUG-ID]
Performance optimization note
Breaking changes (if any) noted separately
Files Modified:
src/main/java/...
src/test/java/...
Changelog.md ? Updated
TechDolg.md  ? Updated (if applicable)
Next Steps Required:
[ ] Manual testing for edge cases
[ ] Code review request submitted
[ ] Documentation update needed (README sections)
[ ] Monitoring/alerting setup required
[ ] Team notification sent
Rollback Procedure (if needed):
git revert HEAD
git push origin develop
Communication Triggers  Request Help When:
Situation       Indicator                                   Action
Blocker         Cannot understand root cause                Stop and request human assistance
Resources       Memory leak detected, OOM                   Halt execution, log details, preserve state
Time            Work exceeding expected >30 min             Check progress, report status, ask guidance
Conflict        Git conflict during merge                   Switch to manual resolution mode
Security        Potential vulnerability found               Alert immediately, do not deploy
Compliance      License/copyright issues                    Pause, verify permissions
? Domain-Specific Patterns for DieselDB
SQL Parser Modifications:
When changing parser components:
Create test input dataset with edge cases
Add test for NULL handling specifically
Validate all three truth statuses: TRUE/FALSE/UNKNOWN
Verify impact on JOIN/ON clause parsing
Ensure backward compatibility with existing queries
Update prompt.md documentation accordingly
Record findings in techdolg.md if non-trivial
Edge Cases to Cover:
Empty WHERE clauses
Quoted identifiers with special characters
Mixed case keywords (select vs SELECT)
Nested parentheses depth
Unicode in strings
Empty table scenarios
Transaction Handling Implementation:
When implementing transactions:
Always test BEGIN ? INSERT ? ROLLBACK pattern
Always test BEGIN ? INSERT ? COMMIT pattern
Verify autoCommit state persists after each operation
Check isolation level enforcement across concurrent access
Test nested transaction attempts (should reject)
Transaction States to Validate:
autoCommit=true (default)
autoCommit=false (after BEGIN)
isInTransaction=true/false flags
SET AUTOCOMMIT = ON/OFF effects
Implicit vs explicit transaction boundaries
Index Implementation Guidelines:
Before committing index changes:
Phase 1: Functional Testing
BTree insert/delete/search operations
Hash collision handling under load
Composite index multi-column functionality
Unique constraint validation through indexes
Phase 2: Performance Validation
Benchmark vs. no-index baseline scenario
Verify index selectivity calculations
Confirm cost-based query planning uses index
Phase 3: Integration Checks
Index works with WHERE clauses
Index supports ORDER BY optimization
Index affects JOIN performance positively
Document expected query types that benefit
Performance Target: Index-backed queries should be ?10ms typical
NULL Three-Valued Logic Implementation:
Validation Requirements:
Column with NULL value returns UNKNOWN for comparison
IS NULL / IS NOT NULL operators always return TRUE/FALSE
TRUE AND UNKNOWN = UNKNOWN ? row excluded from result
FALSE AND UNKNOWN = FALSE ? row excluded from result
UNKNOWN AND UNKNOWN = UNKNOWN ? row excluded from result
NOT UNKNOWN = UNKNOWN ? negation maintains unknown state
Only TRUE results include rows in WHERE filter
Test Scenarios Mandatory:
WHERE COL = NULL ? 0 rows (always)
WHERE COL != NULL ? 0 rows (always)
WHERE COL IS NULL ? matches null values
WHERE COL IS NOT NULL ? matches non-null values
Complex boolean combinations with NULL literals
Subquery returning NULL in IN/clause comparisons
? Metrics to Track
Key Performance Indicators:
Metric                    Goal                        Threshold (Alert)    Measurement
Test Pass Rate            ?100% passing               Drop >10%            per test run
Average Query Time        ?100ms/query                Growth >20%          from timing.md
Active Tech Debt Items    Minimize                    >5 high-priority     in techdolg.md
Code Coverage             ?85%                        Drop <80%            JaCoCo reports
CI Build Duration         <5 minutes                  >10 minutes          GitHub Actions
Commit Frequency          Consistent                  No commits >3 days   git log stats
Flaky Test Count          Zero                        Any occurrence       test classification
Performance Regression    0%                          >20% baseline drop   timing.md comparison
Notification Triggers:
After every successful commit ? automatically update Changelog.md
After any test failure ? create bug-ticket immediately
After optimization work ? compare timing between runs
After major architecture change ? notify team proactively
?? Security Best Practices
NEVER Do These Actions:
? Hardcode credentials (passwords, tokens, API keys)
? Log sensitive user data (PII, tokens, auth cookies)
? Expose internal file paths in stack traces
? Disable validation checks for "performance reasons"
? Store secrets in code comments or documentation
? Use SHA1 for cryptographic purposes
? Allow direct file system writes without validation
? Accept SQL fragments without prepared statements
ALWAYS Implement These Safeguards:
? Sanitize all SQL inputs (use parameterized queries)
? Validate ALL parameters before use (type, range, format)
? Use PreparedStatement where possible throughout
? Encrypt sensitive data at rest (AES-256 recommended)
? Rotate access credentials regularly (every 90 days minimum)
? Implement rate limiting for public-facing endpoints
? Add Content-Security-Policy headers where applicable
? Log security events separately from application logs
Security Audit Checklist (Pre-Merge):
[ ] No hardcoded passwords or keys in source
[ ] Input validation on all external data sources
[ ] SQL injection prevention verified
[ ] Path traversal attacks mitigated
[ ] Error messages don't expose internal details
[ ] Dependencies scanned for vulnerabilities
?? Rollback Strategies & Recovery
Pre-Change Protection:
git stash push -m "backup pre-change $(date +%Y%m%d)"
git tag -a "v-x.x.pre-change-$(date +%Y%m%d)" -m "Backup before changes"
git rev-parse HEAD > commit_backup.txt
Recovery From Problematic Push:
git revert HEAD --no-edit      # Creates reverse commit
git push origin develop        # Push recovery
OR restore from tagged version:
git reset --hard "v-x.x.pre-change-YYYYMMDD"
git push origin develop --force-with-lease  # Safer than --force
Post-Recovery Checklist:
[ ] Notify team of rollback immediately
[ ] Document root cause in ticket system
[ ] Update techdolg.md with lesson learned
[ ] Add prevention measure to workflow process
[ ] Schedule review meeting if critical issue
[ ] Verify rollback didn't introduce new regressions
? Prompt Templates for Qwen Coder
Template 1: Bug Fix Task
TASK: Fix [feature name] bug causing [observed symptom]
CONTEXT:
Test Class: [TestName.java]
Failing Assertion: [error message]
Recent Changes: [last commit affecting area]
REQUIREMENTS:
Create unit test reproducing the issue BEFORE fixing
Locate root cause in source code structure
Implement fix with MINIMAL necessary changes
Verify ALL existing tests still pass
Record fix rationale in techdolg.md
Update Changelog.md with version and description
OUTPUT FORMAT: Provide step-by-step explanation with code snippets
Template 2: New Feature Implementation
TASK: Implement [new feature name]
SCOPE OF AFFECT:
Components affected: [list modules/classes]
Breaking changes: [yes/no]
Dependencies added: [libraries or modules]
TESTING REQUIREMENTS:
Unit tests: [minimum count]
Integration tests: [scenarios]
Edge cases: [specific conditions]
Performance baseline: must not exceed [X ms/query]
TECHNICAL DEBT NOTE:
Document if using temporary solution awaiting improvement
DELIVERABLES CHECKLIST:
? Code implementation
? Unit tests (passing)
? Integration tests (passing)
? Documentation updates
? Changelog entry created
? Tech debt logged (if applicable)
Template 3: Refactoring Task
TASK: Refactor [module/class name]
GOAL: Improve [maintainability/performance/readability/security]
CONSTRAINTS:
DO NOT break existing functionality
Maintain backward compatibility where required
Keep public API signatures stable
TESTING REQUIREMENTS:
Full regression test suite MUST pass
Performance comparison baseline established
No behavioral changes observable externally
DOCUMENTATION UPDATES:
Code comments refreshed
README sections updated
API documentation synchronized
SUCCESS CRITERIA:
Same behavior before and after
Improved code metrics (cyclomatic complexity, LOC)
No performance regression
Template 4: Performance Optimization
TASK: Optimize [query/component/function]
CURRENT STATE:
Baseline timing: [X ms]
Bottleneck identified: [query type/index/memory]
Affected operations: [list]
OPTIMIZATION STRATEGIES TO CONSIDER:
? Index creation/maintenance
? Query restructuring
? Early termination conditions
? Batch processing
? Caching strategy
? Memory allocation improvements
VALIDATION:
Compare against timing.md baseline
Document optimization technique used
Measure improvement percentage
Note any tradeoffs introduced
ACCEPTANCE CRITERIA:
Performance improvement ?20%
No functional regressions
Documentation updated
? Final Quality Gates (Pre-Commit MANDATORY)
Complete Checklist:
TESTING QUALITY
?? mvn test passes without failures (quick tests)
?? mvn -Ptest test passes (if large changes made)
?? All edge cases covered by test assertions
PERFORMANCE STANDARDS
?? Compared against timing.md baseline
?? Degradation ? 20% clearly documented
?? No new performance hotspots introduced
DOCUMENTATION STATUS
?? Changelog.md has entry for THIS version
?? techdolg.md updated if applicable
?? README.md reflects any user-visible changes
?? Code comments reflect new logic added
CODE QUALITY
?? No temporary debug code left behind
?? No unused imports remaining
?? No unused classes/methods present
?? Naming conventions followed consistently
?? Magic numbers replaced with constants
GIT HYGIENE
?? Working on develop branch confirmed
?? Only intentional files committed
?? No target/ or compiled files in staging
?? Staging area contains exactly intended changes
?? Commit message matches changelog entry exactly
SECURITY COMPLIANCE
?? No hardcoded secrets
?? Input validation present on external data
?? Sensitive information not logged
?? Dependencies updated if security patches available
COMMUNICATION READY
?? Status template prepared for announcement
?? Next steps identified clearly
?? Rollback procedure documented (if applicable)
?? Team notification channel identified
Failure Protocol:
If ANY quality gate fails:
Identify which specific gate failed
Document issue in testfail.md with reproducible steps
Attempt correction following domain-specific patterns
Re-run entire workflow from Step 1
If still failing ? escalate to human review
DO NOT proceed past gates until ALL pass
? File Structure Reference
dieseldb/
??? .gitignore                       ? Ignores generated files (CRITICAL)
??? .github/
?   ??? workflows/
?       ??? ci.yml                   ? CI/CD configurations
??? scripts/                         ? Agent helper utilities
?   ??? agent-helper.sh              ? Workflow automation script
??? src/
?   ??? main/java/                   ? Production source code
?   ?   ??? diesel/                  ? Main package structure
?   ??? test/java/
?       ??? diesel/                  ? Test classes
??? target/                          ? BUILD ARTIFACTS (IGNORED)
??? Changelog.md                     ? Version history (COMMIT)
??? README.md                        ? Project documentation (COMMIT)
??? PERSISTENCE_README.md            ? Serialization docs (COMMIT)
??? timing.md                        ? Baseline timings (COMMIT)
??? timing[0-9]*.md                  ? Historical runs (COMMIT)
??? techdolg.md                      ? Technical debt log (COMMIT)
??? testfail.md                      ? Test failure records (COMMIT)
??? prompt.md                        ? Implementation prompts (COMMIT)
??? step.md                          ? Step documentation (COMMIT)
??? pom.xml                          ? Maven configuration (COMMIT)
??? config.properties                ? Server configuration (COMMIT)
??? PROFILES.csv                     ? Test profiles data (COMMIT)
??? USERS.csv                        ? Test data (IGNORED)
??? USER_DETAILS.csv                 ? Test data (IGNORED)
??? *.table                          ? Runtime tables (IGNORED)
? Quick Start Commands
Initial Setup:
git clone https://github.com/Reider85/dieseldb.git
cd dieseldb
git checkout develop
java -version
mvn -version
mvn clean install
Daily Workflow:
git pull origin develop
mvn test
git status
git commit -m "x.x.n: [exact description]"
git push origin develop
Emergency Operations:
git stash push -m "emergency-backup $(date)"
git log --oneline -10
git revert <commit-hash>
git reset --hard v-x.x.stable-tag
? Continuous Improvement
Weekly Review Questions:
Which tech debt items were resolved this week?
Were there any flaky tests? How addressed?
Performance trends stable or degrading?
Any security concerns raised during changes?
What documentation gaps discovered?
Monthly Metrics Review:
Total commits delivered
Test pass rate over time
Tech debt count trend
Performance baseline shifts
Documentation completeness
Quarterly Retrospective:
Process bottlenecks identified
Tool improvements needed
Knowledge gaps to address
Automation opportunities discovered
Success stories to celebrate
? Summary of Improvements Over Basic Agent
This enhanced AGENTS.md includes:
Debugging Tools  commands/scripts for rapid diagnostics
Automation Scripts  bash helpers for routine tasks
Exception Handling  clear algorithms for error states
Status Reporting  unified template for completions
Domain Patterns  diesel-specific guidance (parser/transactions/indexes)
Metrics Tracking  measurable indicators to monitor
Security Checks  prohibited/enforced practices
Rollback Plans  recovery procedures documented
Prompt Templates  ready-to-use task formats
Communication Triggers  escalation criteria defined
Git Safety Protocols  branching/tagging best practices
Quality Gate System  mandatory pre-commit verification
Last Updated: August 2026 | Project: DieselDB v2.7.32 | Maintained by: Reider85 + AI Agents
This document serves as the authoritative guide for all agent interactions with the DieselDB codebase. Follow protocols strictly for consistent, high-quality outcomes.
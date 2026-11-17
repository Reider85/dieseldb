# CI5: Unblock the Release / Tag Pipeline

> **Status**: Diagnostic + remediation plan
> **Predecessor**: `CI4-P4-documentation.md` (repository-level permissions)
> **Scope**: `.github/workflows/{ci,release-on-push,changelog,hybrid-ci}.yml`
> **Goal**: Every push to `main` that touches source code produces a tag + GitHub Release.

---

## 0. Executive Summary — why CI4 fixes did not unblock releases

`CI4-P4-documentation.md` fixed the **repository-level permission** so that the
`GITHUB_TOKEN` can push tags. That fix is correct and necessary, but it is **not
the bottleneck**. The release pipeline is gated on a `workflow_run` chain:

```
push to main
    │
    ▼
┌──────────────────────────────────────────┐
│ ci.yml                                   │
│   compile-gate  ─►  fast                 │   ← fast ALWAYS fails today
│   (other jobs skipped on push)           │
└──────────────────────────────────────────┘
    │ workflow_run event (fires on ANY conclusion)
    ▼
┌──────────────────────────────────────────┐
│ release-on-push.yml                      │
│   if: conclusion == 'success'            │   ← filters out failure
│      && event == 'push'                  │
│      && head_branch == 'main'            │
└──────────────────────────────────────────┘
    │ skipped (job-level `if:` is false)
    ▼
   ❌ no tag, no release
```

### Evidence from the live repository (https://github.com/Reider85/dieseldb)

| Signal | Value | Source |
|---|---|---|
| Tags in repo | **0** | `/tags` page: "There aren't any tags" |
| Releases in repo | **0** | `/releases` page: "There aren't any releases" |
| `release-on-push.yml` runs | 22 total: 5 `failed` (Nov 8–9) → 17 `skipped` (Nov 8–15) | Actions tab |
| `ci.yml` runs (last 15) | **All `failed` or `cancelled`**, none `success` | Actions tab |
| `ci.yml` run #814 (latest) | `compile-gate=success`, **`fast=failure`** (step "Run fast tests", 30m 38s), `ci-failure-issue=success`, `workflow-summary=success` | GitHub API `/actions/runs/44033825440/jobs` |
| `pom.xml` version | `3.4.47` | `pom.xml` line 5 |
| Expected tag (had CI been green) | `v3.4.47-build.814` | release-on-push.yml formula |

### The actual chain of failure

1. **CI4-P1..P3** (Nov 8): `release-on-push.yml` itself was broken → 5 `failed` runs.
2. **CI4-P4** (Nov 8): Fixed repo-level `contents: write` permission → `release-on-push.yml` stopped failing.
3. **CI4-P2** "fix fast profile tests after SLF4J migration": the `fast` job in `ci.yml` is still red on every push. As a result `ci.yml` conclusion is always `failure`.
4. Because `release-on-push.yml` filters on `conclusion == 'success'`, every subsequent run is **skipped** — exactly what we observe for runs #7–22.
5. `CI4-P5/P6/P7/P8` (reorder steps, sync pom version, explicit body, auto-notes) **cannot** take effect: they only run inside the release job, which never starts.

**Conclusion**: CI4 fixed the *ability* to push tags but not the *trigger* that decides whether to push them. CI5 must make `ci.yml`'s `fast` job green AND harden the release chain so a single flake can no longer silently swallow every release.

---

## 1. Identified errors (in priority order)

### ERROR 1 — BLOCKER · `fast` job fails on every push to `main`
* **File**: `.github/workflows/ci.yml`, job `fast`, step `Run fast tests` (lines 73–120).
* **Symptom**: `mvn -B -T 4 … test -P fast` exits non-zero → with `bash -eo pipefail` the step fails → workflow conclusion = `failure`.
* **Evidence**: run #814, `fast` job ran 30m 38s, conclusion `failure`; artifact `reports-fast` (605 KB) exists, proving tests actually executed and one or more asserted/errored.
* **Root cause candidates** (verify against the artifact):
  1. A test tagged `smoke` / `query` / `index` (the groups selected by the `fast` profile in `pom.xml`) fails an assertion.
  2. SLF4J migration left a binding / config mismatch that surfaces only under `mvn test -P fast` (CI4-P2 tried to fix this and did not finish).
  3. `assert-tests-ran.sh "$stamp"` is called with a stamp that was never written to; on a slow disk the `-newer` check can produce a false negative (documented caveat in the script header). This is secondary — the artifact size shows tests did run — but it can mask the real failure message.
* **Why it blocks releases**: `release-on-push.yml` filters on `conclusion == 'success'`; with `fast` red, conclusion is `failure`; release job is skipped.

### ERROR 2 — BLOCKER · `release-on-push.yml` has no `workflow_dispatch` trigger
* **File**: `.github/workflows/release-on-push.yml`, `on:` block (lines 13–16).
* **Symptom**: When the automated chain is broken (as it is now) there is **no manual escape hatch** — you cannot publish a release for a known-good commit.
* **Fix**: Add `workflow_dispatch:` with inputs (`commit_sha`, `version_override`, `dry_run`).

### ERROR 3 — HIGH · `release-on-push.yml` `if:` is all-or-nothing on the upstream conclusion
* **File**: `.github/workflows/release-on-push.yml`, job-level `if:` (lines 30–33).
* **Symptom**: Any red job in `ci.yml` — even `lint-tia-mapping` (which the comment in ci.yml says is `continue-on-error` on push) or `ci-failure-issue` — flips the workflow conclusion to `failure` and skips the release.
* **Fix**: Move the gate decision into a dedicated step inside the release job that calls the GitHub API and inspects the **specific** jobs (`compile-gate`, `fast`) rather than the workflow-level conclusion.

### ERROR 4 — HIGH · `ci.yml` path filter excludes doc-only pushes from CI
* **File**: `.github/workflows/ci.yml`, `push.paths:` (lines 6–15).
* **Symptom**: A push that touches only `**/*.md`, `docs/**`, `LICENSE`, `NOTICE` does not trigger `ci.yml`. No CI run → no `workflow_run` event → `release-on-push.yml` never triggers. The last commit on `main` today (`CI4-P8: enable auto-generated release notes`) is doc-only, so even with a green `fast` job it would not have produced a release.
* **Design note**: The exclusion is intentional (the comment in `release-on-push.yml` documents it), but combined with the absence of a manual trigger it produces silent no-ops.
* **Fix options** (pick one):
  a) Drop the `!**/*.md` exclusion on push so CI runs on every push to `main`.
  b) Keep the exclusion but add `workflow_dispatch` (see ERROR 2) so doc-only pushes can be released manually.
  c) Add a separate `release-on-docs.yml` that builds the javadoc and publishes a Pages artifact + lightweight tag.

### ERROR 5 — HIGH · `changelog.yml` reads a non-existent property `workflow_run.head_ref`
* **File**: `.github/workflows/changelog.yml`, step `Get release tag` (line 50).
* **Code**:
  ```yaml
  TAG="${{ github.event.workflow_run.head_ref }}"
  if [[ "$TAG" == refs/tags/* ]]; then
  ```
* **Bug**: The `workflow_run` event payload exposes `head_branch` and `head_sha`, **not** `head_ref`. Even if it did, `head_branch` for a push to `main` is `main`, not `refs/tags/v…`. The guard `[[ "$TAG" == refs/tags/* ]]` therefore always fails and the step exits 1 with `"ERROR: Expected tag format but got: "` (empty string).
* **Impact**: Even when releases start working, `changelog.yml` will keep failing. It does not block the release itself (it is chained *after* `release-on-push`), but it leaves the changelog un-updated and adds red badges.
* **Fix**: Fetch the latest release tag via `gh release list --json tagName` instead of reading `workflow_run` payload fields.

### ERROR 6 — HIGH · `hybrid-ci.yml` `summary` job has invalid bash (`"" >>` without `echo`)
* **File**: `.github/workflows/hybrid-ci.yml`, job `summary`, step `Generate summary` (lines 205, 208, 212).
* **Code**:
  ```bash
  echo "| GitHub CI | … |" >> $GITHUB_STEP_SUMMARY
  "" >> $GITHUB_STEP_SUMMARY          # ← line 205: missing `echo`
  echo "## CI Mode" >> $GITHUB_STEP_SUMMARY
  "" >> $GITHUB_STEP_SUMMARY          # ← line 208: missing `echo`
  ```
* **Bug**: `""` is an empty string treated as a command → bash returns `command not found` and exits 127. With `bash -eo pipefail` the step fails, which flips the `summary` job red, which flips the whole `Hybrid CI/CD` workflow red.
* **Impact on releases**: Indirect — `release-on-push.yml` listens to the workflow named `CI`, not `Hybrid CI/CD`, so this does not by itself block tags. But it adds noise to the Actions badge, masks real failures, and would block the same chain if the listener were ever repointed.
* **Fix**: Replace every `"" >> $GITHUB_STEP_SUMMARY` with `echo "" >> $GITHUB_STEP_SUMMARY` (3 occurrences).

### ERROR 7 — MEDIUM · No verification that `GITHUB_TOKEN` actually has `contents: write`
* **File**: `.github/workflows/release-on-push.yml`.
* **Symptom**: The workflow declares `permissions: contents: write`, but if the repo setting from CI4-P4 was reverted or never applied, the `git push origin "$TAG"` step fails with `fatal: could not read Username for 'https://github.com'` or a 403. The error is buried 1000 lines into the log.
* **Fix**: Add a first step that calls `gh api repos/$GITHUB_REPOSITORY --jq '.permissions.push'` and fails fast with a clear `::error::` annotation if it is not `true`.

### ERROR 8 — MEDIUM · `ci.yml` `workflow-summary` job has inconsistent YAML indentation
* **File**: `.github/workflows/ci.yml`, step `Generate summary` (lines 826–851).
* **Symptom**: Lines 827–838 use 10 spaces, lines 839–848 use 11 spaces. YAML tolerates this (literal block scalar preserves the extra leading space, bash tolerates leading whitespace), so it is not a syntax error — but it indicates copy-paste drift and makes the next maintainer’s diff noisy. It also means the script that runs is not the script you read.
* **Fix**: Normalise to 10 spaces.

### ERROR 9 — MEDIUM · `Detect orphaned test classes` runs **before** tests in the `fast` job
* **File**: `.github/workflows/ci.yml`, `fast` job, lines 106–113.
* **Symptom**: On a fresh runner `target/test-classes` does not exist → `cls_count=0` → step passes vacuously. After a stale cache restore it can flag orphaned `.class` files that belong to a previous commit and fail the build for the wrong reason.
* **Fix**: Move the step to after `Run fast tests`, or delete it from the `fast` job (the check belongs in `compile-gate`, which is the cheap pre-test gate).

### ERROR 10 — LOW · `Compute release version` relies on `mvn help:evaluate`
* **File**: `.github/workflows/release-on-push.yml`, step `Compute release version` (lines 50–69).
* **Symptom**: `mvn help:evaluate -Dexpression=project.version -q -DforceStdout` is slow (~15–20 s on a cold cache) and the `awk 'NF{p=$0} END{print p}'` filter takes the last non-empty line — if the help plugin prints a warning to stdout it can be captured as the version.
* **Fix**: Parse `pom.xml` directly with `python -c 'import xml.etree.ElementTree as ET; print(ET.parse("pom.xml").getroot().find("{*}version").text)'` or use `mvn` once and write the value to a file.

---

## 2. Prompts to fix each error

Each prompt below is self-contained and can be handed to an LLM or a developer
in order. Prompts 1–4 are blocking; 5–7 are hardening.

---

### Prompt CI5-P1 — Diagnose and repair the failing `fast` profile

**Goal**: Make the `fast` job in `.github/workflows/ci.yml` exit 0 on every push to `main`.

**Steps**

1. Download the `reports-fast` artifact from the latest failed `CI` run (currently run #814, id `44033825440`):
   ```bash
   gh run download 44033825440 --repo Reider85/dieseldb --name reports-fast -D /tmp/reports-fast
   ```
2. Find the failing test(s):
   ```bash
   grep -lE '<failure|<error' /tmp/reports-fast/**/*.xml
   grep -E 'Tests run:.*Failures: [1-9]|Tests run:.*Errors: [1-9]' /tmp/reports-fast/**/*.txt
   ```
3. For each failing class:
   * If the assertion is a real regression → fix the production code or the test.
   * If the failure is order-/timing-dependent → tag the class with `@Tag("flaky")` and add it to the `flaky` profile in `pom.xml`; do **not** leave it in `smoke`/`query`/`index`.
   * If the failure is environmental (port clash, temp dir, locale) → fix the test setup; do not relax the assertion.
4. Reproduce locally:
   ```bash
   mvn -B -T 4 -Dstyle.color=never test -P fast
   ```
   Acceptance: exit code 0, `target/surefire-reports/` populated, no `<failure>` or `<error>` tags.
5. Commit with message `CI5-P1: fix failing fast-profile test (<ClassName>)` and push to `main`.

**Acceptance criteria**
* The next `CI` run on `main` has `fast.result == success`.
* The next `release-on-push.yml` run is **not** skipped.
* `assert-tests-ran.sh` reports `Verified: N surefire report(s) written`.

---

### Prompt CI5-P2 — Add `workflow_dispatch` to `release-on-push.yml`

**Goal**: Allow a manual release for any known-good commit when the automated chain is broken.

**Patch** `.github/workflows/release-on-push.yml`:

```yaml
on:
  workflow_run:
    workflows: [CI]
    types: [completed]
  workflow_dispatch:
    inputs:
      commit_sha:
        description: 'Commit SHA to release (defaults to latest main)'
        required: false
      version_override:
        description: 'Override version suffix (e.g. manual.1). Empty = auto from pom.xml + run number'
        required: false
      dry_run:
        description: 'Dry run (build + log, no tag push, no release)'
        type: boolean
        default: false
```

Update the job-level `if:` so manual dispatch is allowed:

```yaml
if: >-
  github.event_name == 'workflow_dispatch' ||
  (github.event.workflow_run.conclusion == 'success' &&
   github.event.workflow_run.event == 'push' &&
   github.event.workflow_run.head_branch == 'main')
```

Update the checkout `ref:`:

```yaml
- name: Checkout the tested commit
  uses: actions/checkout@v5
  with:
    ref: ${{ github.event.inputs.commit_sha || github.event.workflow_run.head_sha }}
    fetch-depth: 0
```

Update `Compute release version` to honour the override:

```yaml
- name: Compute release version
  id: version
  shell: bash
  run: |
    set -euo pipefail
    BASE=$(mvn -B help:evaluate -Dexpression=project.version -q -DforceStdout | tr -d '\r' | awk 'NF{p=$0} END{print p}')
    case "$BASE" in [0-9]*.[0-9]*.[0-9]*) ;; *) echo "ERROR: bad project.version '$BASE'"; exit 1 ;; esac
    if [ -n "${{ github.event.inputs.version_override }}" ]; then
      VERSION="${BASE}-${{ github.event.inputs.version_override }}"
    else
      RUN="${{ github.event.workflow_run.run_number || github.run_number }}"
      printf '%s' "$RUN" | grep -Eq '^[0-9]+$' || { echo "ERROR: run_number not numeric: '$RUN'"; exit 1; }
      VERSION="${BASE}-build.${RUN}"
    fi
    echo "version=$VERSION" >> "$GITHUB_OUTPUT"
    echo "tag=v${VERSION}" >> "$GITHUB_OUTPUT"
```

Gate the tag-push and release steps on `!inputs.dry_run`:

```yaml
- name: Create and push tag
  if: success() && steps.guard.outputs.exists == 'false' && github.event.inputs.dry_run != 'true'
  …

- name: Create GitHub Release with JAR
  if: steps.guard.outputs.exists == 'false' && github.event.inputs.dry_run != 'true'
  …
```

**Acceptance criteria**
* `gh workflow run release-on-push.yml --repo Reider85/dieseldb -f commit_sha=<sha>` produces a tag + release for that SHA.
* `-f dry_run=true` builds the JAR and prints the tag name without pushing.

---

### Prompt CI5-P3 — Replace the workflow-level conclusion check with a per-job gate

**Goal**: Stop letting an unrelated red job (e.g. `ci-failure-issue`, `lint-tia-mapping`) skip the release.

In `release-on-push.yml`, relax the job-level `if:` to only filter on event/branch:

```yaml
if: >-
  github.event_name == 'workflow_dispatch' ||
  (github.event.workflow_run.event == 'push' &&
   github.event.workflow_run.head_branch == 'main')
```

Add a new step right after checkout that calls the GitHub API and inspects the two jobs that actually gate a push release:

```yaml
- name: Verify upstream CI jobs
  id: upstream
  if: github.event_name != 'workflow_dispatch'
  env:
    GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    RUN_ID: ${{ github.event.workflow_run.id }}
  run: |
    set -euo pipefail
    for job in compile-gate fast; do
      conclusion=$(gh api repos/$GITHUB_REPOSITORY/actions/runs/$RUN_ID/jobs \
        --jq ".jobs[] | select(.name == \"$job\") | .conclusion")
      echo "$job=$conclusion" >> "$GITHUB_OUTPUT"
      echo "Upstream job $job: $conclusion"
      [ "$conclusion" = "success" ] || { echo "::error::Upstream job $job is $conclusion — refusing to release"; exit 1; }
    done
```

**Acceptance criteria**
* A push where `compile-gate=success`, `fast=success`, `ci-failure-issue=failure` still produces a release.
* A push where `fast=failure` produces a clear `::error::` annotation and no release.

---

### Prompt CI5-P4 — Repair `changelog.yml` tag detection

**Goal**: Make `changelog.yml` find the release tag reliably.

Replace the `Get release tag` step (`.github/workflows/changelog.yml` lines 45–62) with:

```yaml
- name: Get release tag
  id: get-tag
  env:
    GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: |
    set -euo pipefail
    TAG=$(gh release list --repo "$GITHUB_REPOSITORY" --limit 1 --json tagName --jq '.[0].tagName // empty')
    if [ -z "$TAG" ]; then
      echo "No published release found — nothing to changelog."
      exit 0
    fi
    VERSION="${TAG#v}"
    BASE_VERSION="${VERSION%-build.*}"
    echo "tag=$TAG"        >> "$GITHUB_OUTPUT"
    echo "version=$VERSION" >> "$GITHUB_OUTPUT"
    echo "base_version=$BASE_VERSION" >> "$GITHUB_OUTPUT"
    echo "Release tag: $TAG (base $BASE_VERSION)"
```

Also tighten the `if:` at the job level (line 24–26): the current filter
`event == 'push'` rejects manual dispatch of the release workflow. Replace
with `github.event.workflow_run.conclusion == 'success'` only.

**Acceptance criteria**
* After a successful `release-on-push.yml` run, `changelog.yml` runs and
  prints `Release tag: v3.4.47-build.N (base 3.4.47)`.
* `Changelog.md` is updated and committed with `[skip ci]`.

---

### Prompt CI5-P5 — Fix `hybrid-ci.yml` `summary` job bash syntax

**Goal**: Stop the `summary` job from failing with `command not found`.

In `.github/workflows/hybrid-ci.yml`, job `summary`, step `Generate summary`,
replace **all three** occurrences of:

```bash
          "" >> $GITHUB_STEP_SUMMARY
```

with:

```bash
          echo "" >> $GITHUB_STEP_SUMMARY
```

(Lines ~205, ~208, ~212 — verify with `grep -n '"" >>' .github/workflows/hybrid-ci.yml`.)

**Acceptance criteria**
* `grep '"" >>' .github/workflows/hybrid-ci.yml` returns no matches.
* The next `Hybrid CI/CD` run has `summary.result == success`.

---

### Prompt CI5-P6 — Add a "release health" diagnostic workflow

**Goal**: Make the failure mode visible without manually correlating three workflows.

Create `.github/workflows/release-health.yml`:

```yaml
name: Release Health
on:
  schedule:
    - cron: '0 6 * * *'      # daily 06:00 UTC
  workflow_dispatch:

permissions:
  actions: read
  contents: read
  issues: write

jobs:
  diagnose:
    runs-on: ubuntu-24.04
    timeout-minutes: 5
    steps:
      - name: Diagnose release pipeline
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -uo pipefail
          {
            echo "## Release Health Report — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
            echo ""
            echo "| Check | Value |"
            echo "|---|---|"
          } > report.md

          LATEST_CI=$(gh run list --repo "$GITHUB_REPOSITORY" --workflow=ci.yml --branch=main --limit=1 --json conclusion,headSha,databaseId --jq '.[0]')
          CI_CONCL=$(echo "$LATEST_CI" | jq -r '.conclusion // "unknown"')
          CI_SHA=$(echo "$LATEST_CI"   | jq -r '.headSha // "?"')
          echo "| Latest CI on main | \`$CI_CONCL\` @ \`$CI_SHA\` |" >> report.md

          LATEST_REL=$(gh run list --repo "$GITHUB_REPOSITORY" --workflow=release-on-push.yml --limit=1 --json conclusion,status --jq '.[0]')
          REL_CONCL=$(echo "$LATEST_REL" | jq -r '.conclusion // "unknown"')
          echo "| Latest release-on-push | \`$REL_CONCL\` |" >> report.md

          LATEST_TAG=$(gh api "repos/$GITHUB_REPOSITORY/releases/latest" --jq '.tag_name' 2>/dev/null || echo "none")
          echo "| Latest published release | \`$LATEST_TAG\` |" >> report.md

          {
            echo ""
            if [ "$CI_CONCL" != "success" ]; then
              echo "### ⚠️ CI is not green on main (\`$CI_CONCL\`)"
              echo "Releases are blocked because release-on-push requires upstream success."
              echo "Action: fix the \`fast\` job (CI5-P1)."
              HEALTHY=1
            elif [ "$LATEST_TAG" = "none" ]; then
              echo "### ⚠️ CI green but no release published"
              echo "Action: inspect release-on-push.yml logs; run CI5-P7 permission check."
              HEALTHY=1
            else
              echo "### ✅ Release pipeline healthy"
              HEALTHY=0
            fi
          } >> report.md

          cat report.md >> "$GITHUB_STEP_SUMMARY"

          # Persist for the issue step
          echo "healthy=$HEALTHY" >> "$GITHUB_ENV"

      - name: Open issue if unhealthy
        if: env.healthy == '1'
        uses: peter-evans/create-issue-from-file@v5
        with:
          title: "Release pipeline unhealthy — $(date -u +%Y-%m-%d)"
          body-filepath: report.md
          labels: ci,release-blocker
```

**Acceptance criteria**
* The workflow runs daily and writes a step summary.
* On the first run after applying CI5-P1..P3, it prints `✅ Release pipeline healthy`.

---

### Prompt CI5-P7 — Verify `GITHUB_TOKEN` write permission before tag push

**Goal**: Fail fast with a clear message when repo settings revoke `contents: write`.

In `release-on-push.yml`, add as the **first** step after `actions/checkout`:

```yaml
- name: Verify token can push tags
  env:
    GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: |
    set -euo pipefail
    PERM=$(gh api "repos/$GITHUB_REPOSITORY" --jq '.permissions')
    echo "Repository permissions: $PERM"
    CAN_PUSH=$(echo "$PERM" | jq -r '.push')
    if [ "$CAN_PUSH" != "true" ]; then
      echo "::error::GITHUB_TOKEN lacks push permission. Enable Settings → Actions → General → Workflow permissions → 'Read and write permissions'."
      exit 1
    fi
    echo "✅ Token can push — tag push will succeed."
```

**Acceptance criteria**
* If the repo setting is reverted, the workflow fails at this step with a clear annotation pointing to the setting to fix, instead of failing 1000 lines later at `git push origin`.

---

### Prompt CI5-P8 — Clean up `ci.yml` `fast` job and `workflow-summary` indentation

Two small fixes that reduce noise and prevent future false negatives:

1. **Remove `Detect orphaned test classes` from the `fast` job** (ci.yml lines 106–113). The check belongs in `compile-gate`; running it before `mvn test` produces vacuous passes on fresh runners and false failures on stale caches.
2. **Normalise indentation** of the `workflow-summary` step (ci.yml lines 839–848): change the 11-space indent back to 10 spaces so the script you read is the script that runs.

**Acceptance criteria**
* `grep -n 'Detect orphaned' .github/workflows/ci.yml` returns matches only in `compile-gate` and PR/nightly jobs, not in `fast`.
* `awk 'NR>=826 && NR<=851 { n=match($0,/[^ ]/); print NR, n-1 }' .github/workflows/ci.yml` shows a single value of leading-space count for the `echo` lines.

---

## 3. Suggested application order

| # | Prompt | Unblocks releases? | Risk | Effort |
|---|---|---|---|---|
| 1 | CI5-P1 (fix `fast` tests) | ✅ directly | low (test-only change) | medium (need to read artifact) |
| 2 | CI5-P2 (add `workflow_dispatch`) | ✅ escape hatch | low | small |
| 3 | CI5-P7 (verify token perms) | early failure signal | low | small |
| 4 | CI5-P3 (per-job gate) | robustness | medium (changes trigger semantics) | small |
| 5 | CI5-P4 (fix changelog tag) | unblocks changelog | low | small |
| 6 | CI5-P5 (fix hybrid-ci bash) | removes noise | low | trivial |
| 7 | CI5-P6 (release-health workflow) | observability | low | small |
| 8 | CI5-P8 (ci.yml cleanup) | robustness | low | trivial |

Apply 1 → 2 → 7 first; this is the smallest change set that produces a tag on
the next push to `main`. Apply 3 → 4 → 5 → 6 → 8 in a follow-up PR to harden
the pipeline.

---

## 4. Verification checklist

After applying CI5-P1 through CI5-P3 + CI5-P7:

- [ ] `mvn -B test -P fast` exits 0 locally on `main`.
- [ ] The next push to `main` that touches `src/**` produces a `CI` run with `compile-gate=success` AND `fast=success`.
- [ ] The `release-on-push.yml` run for that CI run is **not** skipped.
- [ ] A tag `v3.4.47-build.<N>` appears under `https://github.com/Reider85/dieseldb/tags`.
- [ ] A GitHub Release `v3.4.47-build.<N>` appears under `https://github.com/Reider85/dieseldb/releases` with the JAR attached.
- [ ] `gh workflow run release-on-push.yml -f dry_run=true` builds the JAR and prints the tag without pushing.
- [ ] `gh workflow run release-on-push.yml -f commit_sha=<known-good-sha>` produces a release for that SHA even if `main` has moved on.
- [ ] The `release-health.yml` daily run reports `✅ Release pipeline healthy`.

After applying CI5-P4 through CI5-P8:

- [ ] `changelog.yml` runs after each release and commits to `Changelog.md` with `[skip ci]`.
- [ ] `hybrid-ci.yml` `summary` job is green.
- [ ] `grep '"" >>' .github/workflows/hybrid-ci.yml` returns nothing.
- [ ] `release-on-push.yml` fails within 30 seconds with a clear `::error::` if repo permissions are revoked.
- [ ] No `Detect orphaned test classes` step exists in the `fast` job.

---

## 5. What CI4 got right and what it missed

| | CI4 (P1–P8) | CI5 |
|---|---|---|
| Fixed repo-level `contents: write` permission | ✅ | (kept) |
| Fixed `release-on-push.yml` step ordering | ✅ | (kept) |
| Synced `pom.xml` version | ✅ | (kept) |
| Made release body explicit | ✅ | (kept) |
| Enabled auto-generated release notes | ✅ | (kept) |
| **Diagnosed why `fast` is red** | ❌ | ✅ CI5-P1 |
| **Added manual dispatch escape hatch** | ❌ | ✅ CI5-P2 |
| **Replaced workflow-level conclusion gate with per-job gate** | ❌ | ✅ CI5-P3 |
| **Fixed `changelog.yml` `head_ref` bug** | ❌ | ✅ CI5-P4 |
| **Fixed `hybrid-ci.yml` `"" >>` bash bug** | ❌ | ✅ CI5-P5 |
| **Added release-health observability** | ❌ | ✅ CI5-P6 |
| **Added fail-fast permission verification** | ❌ | ✅ CI5-P7 |
| **Cleaned up `fast` job vacuous steps** | ❌ | ✅ CI5-P8 |

CI4 made the release workflow **able** to push tags. CI5 makes the chain
**actually fire** and **fail loudly** when it cannot.

---

*End of CI5.*

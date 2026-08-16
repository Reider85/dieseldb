# AGENTS.md

DieselDB: an experimental file-persisted SQL database in Java (package-private engine, ~39 classes in `diesel/`), driven prompt-by-prompt from `prompt2.md` (stage‑1, 100 prompts).  
Each prompt ends with a Changelog entry + commit + push. Remote: `github.com/Reider85/dieseldb.git`.

---

## AI Agent Quick Start (opencode/kilocode)

**Workflow for each prompt:**

1. Read `PROMPT_STATUS.md` → select next TODO with highest priority.
2. Read `prompt2.md` → find detailed prompt description.
3. Implement changes.
4. Run **full acceptance gate** (includes heavy joins) with `make timing` – this automatically:
  - Builds the project
  - Runs the full test suite with `@LargeTest` and 4GB heap
  - Generates a new timing file (`timingN.md`)
  - Compares it against the baseline `timing.md` **and fails if real degradation** (see below)
5. Run profile check: `make check-profile` – compares the two 360k‑row joins against the previous version (stored in `Changelog.md`). Fails if degradation >10%.
6. Create changelog entry: `make changelog "short description of changes"` – this script auto‑appends test results, timing summary, and profile numbers. You only provide the one‑sentence description.
7. Commit with `git commit -F changelog_entry.txt` (the script creates this file).
8. Update `PROMPT_STATUS.md` → mark as DONE.

**Priority Queue (Pareto 20% → 80% results):**
| № | Priority | Problem | Files |
|---|----------|---------|-------|
| 1 | CRITICAL | JOIN OR → OOM | SelectQuery.java, QueryParser.java |
| 5 | CRITICAL | IN+AND ignored | QueryParser.java |
| 3 | HIGH | GROUP BY unique → 1 row | SelectQuery.java |
| 22 | HIGH | NPE in 13 places | Multiple |
| 29 | HIGH | Complexity=59 refactor | SelectQuery.java |

---

## Build & run (Windows)

- Maven is NOT on `PATH`. Use the IntelliJ‑bundled Maven:  
  `& "C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2023.3.1\plugins\maven\lib\maven3\bin\mvn.cmd" <args>`  
  (copy also at `C:\tools\apache-maven-3.9.6\bin\mvn.cmd`).
- Set `JAVA_HOME` inline to JDK 21:  
  `$env:JAVA_HOME = "C:\Program Files\Axiom\AxiomJDK-21"`  
  (JDK 17 no longer compiles – pom.xml requires 21).
- `mvn package` produces a jar without a usable `Main-Class`; launch the engine via `diesel.DatabaseServer` or use `start-server.bat/.sh`.
- Build: `make build` (or `mvn package -DskipTests`).

## Tests

- All acceptance tests are run via **`make timing`** – this is the single entry point for the agent.
- `make timing` sets `-Ddiesel.largeTests=true -Dtest.heap=4g` and runs the full suite. It records timings to `timingN.md` and immediately compares to the baseline.
- For quick local checks (without heavy joins) you may use `mvn test` (skips `@LargeTest`), but **this is NOT part of the agent workflow** – always use `make timing` before commit.
- The gate expects `Failures: 0, Errors: 0`. The script `compare-timing.sh` will automatically ignore sub‑11ms micro‑queries and only treat degradation >20% on **heavy (>100ms)** queries as a failure. If heavy queries are stable, the script returns exit code 0.

---

## Timing regression check (fully automated)

- `make timing`:
  1. Builds and runs the full test suite (including two 600×600 ORDER BY joins).
  2. Generates `timingN.md` in the repo root.
  3. Executes `compare-timing.sh timing.md timingN.md` – this script:
    - Compares each query’s time.
    - **Ignores** any query whose baseline is <11 ms (machine noise).
    - Flags a regression only if a query with baseline ≥11 ms degrades by >20%.
    - Exits with code `1` if any such regression exists, otherwise `0`.
- You never need to interpret the output manually – just check the exit code. If the script fails, rerun `make timing` once (to rule out random noise) and if it still fails, investigate.
- `timing.md` is the tracked baseline – never delete it.

---

## Profile check (fully automated)

- Driver: `ProfileMain.java` (outside repo) runs the two critical 360k‑row joins. It writes structured results to `profile_results.json`.
- `make check-profile`:
  1. Compiles `ProfileMain.java` against `target/classes`.
  2. Runs it with `-Xmx4g`.
  3. Parses the last profile numbers from `Changelog.md` (looks for the previous prompt’s entry).
  4. Compares the new numbers with the old ones.
  5. Exits with code `1` if any of the two joins degrades by >10%, otherwise `0`.
- The agent only runs `make check-profile` and checks the exit code – no manual grepping or mental arithmetic.

---

## Changelog automation

- Use `make changelog "your change description"` – for example:
  ```bash
  make changelog "Fix JOIN OR OOM by implementing hash join spilling"
# Contributing to DieselDB

Thank you for your interest in contributing to DieselDB! This document explains how to report issues, set up a development environment, run the test gates, and submit pull requests.

By submitting a contribution, you agree that it will be licensed under the [Apache License 2.0](LICENSE), the same license that covers the project (Section 5 of the Apache-2.0 terms).

---

## Table of Contents

- [Reporting Issues](#reporting-issues)
- [Development Setup](#development-setup)
- [Workflow](#workflow)
- [Test Gates](#test-gates)
- [Commit Message Format](#commit-message-format)
- [Code Style](#code-style)
- [Priority Areas](#priority-areas)
- [Pull Request Process](#pull-request-process)

---

## Reporting Issues

- Use [GitHub Issues](https://github.com/Reider85/dieseldb/issues) for bug reports and feature requests.
- Search existing issues first to avoid duplicates.
- For bugs: include the DieselDB version (`pom.xml`), JDK version, OS, storage backend (CSV/TSV/JSONL/AVRO/in-memory), the exact SQL to reproduce, and the observed vs. expected behavior.
- For security issues, please do not open a public issue — contact the maintainer directly (Reider85 / Vasiliy Zhila).

## Development Setup

| Requirement | Version |
|---|---|
| JDK | 21 (LTS) |
| Maven | 3.9+ (only needed if calling `mvn` directly) |
| Build driver | `make` (GNU Make), or `scripts\make.ps1` on Windows |

**Windows note:** this project runs on Windows. `mvn` is not on `PATH` — always prefer the `make` targets (or `scripts\make.ps1`) so `JAVA_HOME` is set correctly. Never call `mvn` directly without setting `JAVA_HOME` first.

Quick start:

```bash
git clone https://github.com/Reider85/dieseldb.git
cd dieseldb
make compile     # verify it compiles
make test        # fast test suite
```

On Windows PowerShell:

```powershell
.\scripts\make.ps1 compile
.\scripts\make.ps1 test
```

## Workflow

1. Fork the repository and create a feature branch: `git checkout -b feature/my-feature`
2. Make your changes in `diesel/` (engine) and/or `src/test/java/diesel/` (tests).
3. Add or update tests for every behavior change.
4. Run the relevant test gates (see below).
5. Commit using the project's commit message format.
6. Open a Pull Request against `main`.

### Test gates by change type

| Change type | Fast gate | Acceptance gate |
|---|---|---|
| Rename, comment-only, constant extraction | `make compile` → `make test-incr` | skip |
| New feature, query logic, bugfix | `make test` | `make timing-quick` |
| JOIN, hash join, performance, schema change | `make test` | `make timing-quick` + `make check-profile` |
| WAL / MVCC / recovery / checkpoint / storage-format | `make test` | skip `make timing-quick` (not on query path) |
| Test infrastructure / build target | `make compile` → `make test-incr` | the change's own acceptance checks |

- **Isolate failures first:** `make test-one T=TestClassName#methodName`, fix, re-isolate, then spend a full `make test` only after the isolated test is green.
- **TIA:** `make tia` prints which Maven profiles your diff impacts — run those instead of the whole suite when the mapping says so.
- **Timing:** `make timing-quick` (3–5 min) gates query-path changes; only a degradation > 20% on queries with a ≥ 2 s baseline fails the gate. `make timing-full` (10–30 min) is reserved for schema change / hash-join refactor work.
- Never run test commands without a Maven profile — the default surefire config excludes all tests and a "0 tests ran" run still prints `BUILD SUCCESS`.

## Commit Message Format

Commits follow the version-prefix format used throughout the history:

```
X.Y.Z Short description of the change
```

Prompt-driven commits add the prompt reference:

```
3.3.32 Prompt4 #37: online schema changes step 1/3
```

For routine work the release target computes the version for you:

```bash
make release DESC="short description of changes"
```

This appends the changelog entry, syncs the `pom.xml` version, commits, and pushes. Use `make release-local DESC="..."` to commit without pushing.

**Never commit** `target/`, `data/`, `logs/`, or `timing/` — they are gitignored build/runtime artifacts. Always push to `origin main` (never `master`), and never force-push.

## Code Style

- **Java 21 (LTS)** — records, sealed classes, pattern matching where applicable.
- **Logging:** SLF4J + Logback; no `System.out.println` in engine code.
- **Tests:** JUnit Jupiter 5.10, Awaitility 4.2 for async conditions.
- **Null-safety:** JSR-305 annotations (`@Nullable` / `@Nonnull`).
- **No UTF-8 BOM in `.java` files** — PowerShell 5.1 `Set-Content -Encoding UTF8` writes a BOM and javac fails with `illegal character: '\ufeff'`.
- **Locale:** use `Locale.ROOT` in formatting/parsing code (ru-RU breaks decimal assertions).
- **Database lifecycle:** any test that constructs `Database` MUST call `database.close()` in `@AfterEach`/`@AfterAll` (null-guarded) — open handles block `@TempDir` cleanup.
- **Timing assertions:** wall-clock/microsecond assertions must run `@Isolated` (or use Awaitility); do not relax thresholds to silence load-related reds.
- **SonarQube:** run `mvn sonar:sonar` for static analysis on larger changes.

## Priority Areas

- **Critical:** complete stub implementations (LogicalDump, LogicalRestore, TablespaceRegistry, PhysicalBackup.triggerCheckpoint)
- **High:** polish TupleVisibility semantics (RC vs RR vs S), add FOREIGN KEY / CHECK / NOT NULL / DEFAULT constraints
- **Medium:** add JSON column type, CTE, window functions, UNION/INTERSECT/EXCEPT
- **Performance:** parallel single-query execution, connection pooling, cost-based optimizer
- **Ecosystem:** jOOQ support, Docker image, K8s operator, GUI client
- **Governance:** `CODE_OF_CONDUCT.md`, `SECURITY.md`

## Pull Request Process

1. Keep PRs focused — one feature or fix per PR; prompt-sized changes are the norm in this repository.
2. Ensure `make compile` and the relevant test gates are green before requesting review.
3. Update documentation (`README.md`, `PROMPT_STATUS.md` where applicable) when behavior changes.
4. A maintainer will review; squash-merge is used on `main`.
5. Releases are handled by the maintainer via `make release`.

---

*DieselDB is developed by Reider85 (Vasiliy Zhila) together with AI agents (qwen-intl, openhands-agent) in an AI-driven, prompt-by-prompt workflow. See [README.md](README.md) for the full project documentation.*

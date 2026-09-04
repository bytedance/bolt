# AGENTS.md

Guidance for coding agents working in the Bolt repository.

## Overview

Bolt is a C++ data-processing and query-execution library built with Conan and
CMake. Read [CONTRIBUTING.md](CONTRIBUTING.md) and
[doc/coding-style.md](doc/coding-style.md) before making changes. The rules
below summarize constraints that are easy to violate during automated edits.

## Branch Hygiene

- Inspect the current branch, remotes, and working tree before editing.
- Create a topic branch from an up-to-date `upstream/main`. Do not rebase,
  force-push, delete branches, or rewrite existing user work unless explicitly
  requested.
- Preserve unrelated staged, unstaged, and untracked changes. Keep each branch
  focused on one reviewable concern.

## Build and Testing

```bash
make debug
make release
make unittest
make unittest_release
```

Build outputs are under `_build/<BuildType>`. Reconfigure the build after
adding source files, changing CMake targets, or changing build options. Run the
smallest relevant test target while iterating, then validate every affected
build mode before finishing.

### Test targets

- GoogleTest unit test targets must link `bolt_gtest_main`. It supplies the
  standard test entry point and the public `bolt_testutils` dependency.
- Do not define `main()` in individual GoogleTest files or compile another copy
  of the test entry point.
- Consume test utilities through `bolt_gtest_main` or `bolt_testutils`. Do not
  collect or link the object libraries that implement `bolt_testutils`.
- Select shared or static test linkage with `BOLT_TEST_LINKAGE`. Do not
  hard-code an engine or testutils implementation target in an individual
  test.

## Build-Mode Configuration

- Do not reintroduce a global `SPARK_COMPATIBLE` compile definition.
- Keep build-mode-dependent behavior in the implementation files that need it.
  Use the configured `kSparkCompatible` constant for C++ branches and CMake
  source selection when a complete source file belongs to one mode.
- Do not include build-mode configuration from public or widely included
  headers unless a template definition must vary. Keep unavoidable template
  logic in the narrowest internal or `-inl.h` header.
- Preserve runtime behavior in both Presto and Spark modes. Verify that
  switching modes does not rebuild unrelated translation units.

## Command-Line Flags

- A gflag used by more than one source file must have one owner. Put the only
  `DECLARE_*` in the owner's header and the only `DEFINE_*` in its source file.
  Consumers include the owner header and use `FLAGS_xxx`.
- Keep a `DEFINE_*` local to a source file only when that file builds an
  independent binary and the flag is used nowhere else. Both conditions are
  required.
- If a local flag gains a second consumer, move it to an owner header and
  source file instead of adding declarations beside each consumer.
- Use the existing shared owners when applicable:
  - Production: `bolt/common/flags/BoltFlags.h` and `BoltFlags.cpp`.
  - Tests: `bolt/common/testutil/BoltTestFlags.h` and `BoltTestFlags.cpp`.
  - Fuzzers: `bolt/exec/fuzzer/FuzzerFlags.h` and `FuzzerFlags.cpp`.
- Prefix new flag names by purpose: `bolt_` for production,
  `bolt_testing_` for tests, `bolt_fuzzer_` for fuzzers, and
  `bolt_benchmark_` for benchmarks.

## Formatting

```bash
pre-commit run --files <paths>
```

Use the repository hooks for C++, CMake, Python, shell, license, and secret
checks. Do not bypass a failing hook unless the user explicitly requests it and
the skipped check has been validated separately.

## Commit Messages

- Each commit must be an atomic, independently applicable and testable unit of
  work.
- Write a concise English title using `type: subject`. Common types are
  `feat`, `fix`, `docs`, `test`, `build`, `ci`, `perf`, and `refactor`.
- Use the body to explain motivation, important design decisions, and notable
  compatibility or performance effects. Do not repeat the diff line by line.
- Do not add AI attribution, generated-by text, or an agent as a co-author.
- Review the staged diff and run the relevant hooks and tests before committing.

## Issues

- Pick the matching form under
  [.github/ISSUE_TEMPLATE/](.github/ISSUE_TEMPLATE/) (bug report, build issue,
  compatibility report, documentation issue, feature request, performance
  issue, chore) and mirror its fields as the issue body headings. Fill every
  required field; a compatibility report needs a minimal self-contained
  reproduction plus an explicit expected-vs-actual comparison.
- Keep the template's title prefix (e.g. `[Compatibility] `) and make the rest
  a short one-line summary, not a full sentence describing the whole finding.
- Apply the labels the template declares; skip any label that does not exist in
  the repository instead of failing or inventing new ones.
- Sanitize issue content before posting: no internal application IDs, cluster
  or dashboard URLs, table or column names, business keys, or internal build
  numbers. Reproduce with self-contained literals instead.
- Do not create, edit, or close issues unless the user authorizes that
  external action.

## Pull Requests

- Push topic branches to the contributor fork and open PRs against the intended
  upstream base. Do not push directly to the upstream repository.
- Use [.github/pull_request_template.md](.github/pull_request_template.md) and
  complete every applicable section: problem, change type, description,
  performance impact, release note, validation, and breaking changes.
- Keep the PR small and focused. Separate unrelated cleanup, formatting, or
  infrastructure work into another PR.
- Report only tests and benchmarks that were actually run. Include reproducible
  commands, build options, failures, retries, and any remaining validation gap.
- Use a draft PR while required validation is incomplete. Do not create, update,
  mark ready, or merge a PR unless the user authorizes that external action.

## PR Review

- Treat review requests as read-only unless fixes are explicitly requested.
- Review the complete branch diff against the stated base. Prioritize semantic
  drift, correctness, compatibility, build scope, performance regressions,
  duplicated logic, and unnecessary change volume.
- Support findings with exact file and line references and verify suspected
  failures when practical. Separate confirmed defects from risks and questions.
- Do not post GitHub reviews or comments without explicit authorization.

## Common Mistakes

- Adding a custom GoogleTest `main()` or linking testutils object targets.
- Duplicating `DEFINE_*` or scattering `DECLARE_*` across consumers.
- Putting build-mode conditionals in high-fanout headers.
- Expanding a focused change with unrelated cleanup or formatting churn.
- Claiming a failure is caused by a change without reproducing or isolating it.

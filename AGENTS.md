# AGENTS.md

Guidance for TRAE CLI and other coding agents working in the Bolt repository.

## Agent Resources

Project-specific reusable workflows live under `.trae/skills/`. The matching
Codex-compatible entries under `.agents/skills/` are relative symlinks to those
files; edit the `.trae/skills/` source files.

- Use `bolt-query` when answering architecture, implementation, or PR questions.
- Use `bolt-pr-review` when reviewing a PR, merge request, or local diff.
- Use `bolt-write-commit-message` when drafting or rewriting a Bolt commit
  message.
- Use `bolt-ci-failure-analysis` when investigating failed GitHub Actions or
  internal Codebase CI runs.
- Use `bolt-build` for configuring, compiling, and choosing narrow CMake
  targets.

## Branch Hygiene

Before creating a branch or worktree, inspect the current state:

```bash
git status --short --branch
git worktree list --porcelain
git remote -v
```

Do not assume `origin` points to GitHub. This repository is often used with both
GitHub and internal Codebase remotes. Base new work on the branch or remote the
user asked for; otherwise prefer `main`.

Keep unrelated worktrees and user changes intact. Do not reset, checkout, or
remove changes you did not make unless the user explicitly asks.

## Overview

Bolt is a C++17 acceleration library for composable, extensible, and performant
data processing. It provides a physical execution layer for engines such as
Spark, Flink, Presto, and OpenSearch, and for storage formats such as Parquet,
ORC, text, CSV, Paimon, and Lance.

Bolt-specific build options, namespaces, error macros, CI, and contribution
rules are authoritative. Prefer local Bolt documentation over assumptions from
other projects.

## Build

Bolt uses Conan 2 and a root Makefile. Build artifacts live under
`_build/<BuildType>`.

Common entry points:

```bash
make debug
make release
make release_with_test
make release_spark
make benchmarks-build
```

For routine validation after a build has already been configured, use the
narrowest CMake target through the active Conan preset:

```bash
cmake --build --preset conan-release --target <target>
cmake --build --preset conan-debug --target <target>
cmake --build --preset conan-relwithdebinfo --target <target>
```

Check `_build/.build_type` before deciding whether a reconfiguration is needed.
Reconfigure only when the build type, enabled tests, benchmark options, or target
set needs to change.

## Testing

Unit tests use GoogleTest and CTest. Test files normally live in a module's
`tests/` directory next to the related source.

Common commands:

```bash
make unittest
make unittest_release
make unittest_release_spark
ctest --test-dir _build/Release --timeout 7200 -j 8 --output-on-failure
ctest --test-dir _build/Debug --timeout 7200 -j 8 --output-on-failure
```

For a focused test, run the relevant test binary directly with
`--gtest_filter=<Suite.Test>` after locating it under `_build/<BuildType>/`.

Bug fixes and new features should include focused regression coverage unless the
change is documentation-only or purely mechanical.

New unit tests should use shared test utilities and `bolt_gtest_main`. Avoid
custom `main()` functions, direct links to internal object targets, and
build-mode conditionals in public headers.

## Benchmarks

Build benchmark targets with:

```bash
make benchmarks-basic-build
make benchmarks-build
make benchmarks-build-spark
```

For performance-sensitive changes, do not rely on a single run or a raw average.
Compare baseline and candidate with repeated, alternating runs, report the test
data and exact commands, and separate noise from meaningful regressions.

## Formatting And Static Analysis

Bolt uses `clang-format-14`, pre-commit hooks, and `scripts/run-clang-tidy.py`.

```bash
make clang-format-check
clang-format -i -style=file path/to/file.cpp
python3 scripts/run-clang-tidy.py --commit origin/main bolt/
```

Do not reformat unrelated legacy files in the same change. Keep style-only edits
separate from behavioral work unless they are required for the touched lines.

## Coding Style

Read `doc/coding-style.md` and `CONTRIBUTING.md` for the full rules. Key points:

- Use C++17 and existing Bolt abstractions before introducing new ones.
- Use `BOLT_CHECK_*` for internal errors and `BOLT_USER_CHECK_*` for user
  errors.
- Prefer comparison-specific checks such as `BOLT_CHECK_LT(idx, size)`.
- Use `BOLT_FAIL()`, `BOLT_USER_FAIL()`, `BOLT_UNREACHABLE()`, and `BOLT_NYI()`
  for the corresponding unconditional paths.
- Use PascalCase for types and file names, camelCase for functions and
  variables, camelCase_ for private or protected members, snake_case for
  namespaces and build targets, UPPER_SNAKE_CASE for macros, and kPascalCase for
  static constants and enumerators.
- Prefer uniform initialization and declare variables in the smallest practical
  scope.
- New source files must include the Apache 2.0 license header from
  `license.header`.

## Gflags

Shared flags need one owner header and one owner implementation: keep the only
`DECLARE_*` in the owner `.h`, keep the only `DEFINE_*` in the owner `.cpp`, and
have consumers include that header. Do not duplicate `DECLARE_*` in consumers or
duplicate `DEFINE_*` anywhere.

Keep `DEFINE_*` in the current `.cpp` only when that file is an independent
binary and the flag is used only in that file. If a private flag later gains a
second consumer, move it to an owner `.h/.cpp`.

Common owners are `bolt/common/flags/BoltFlags.h/.cpp`,
`bolt/common/testutil/BoltTestFlags.h/.cpp`, and
`bolt/exec/fuzzer/FuzzerFlags.h/.cpp`. Module-local shared flags may have their
own owner pair. Prefix new flags with `bolt_`, `bolt_testing_`, `bolt_fuzzer_`,
or `bolt_benchmark_` according to use.

## Comments

Comments should explain information that is not obvious from the code. Use `///`
for Doxygen-style public API documentation in headers and `//` for ordinary code
comments. Comments should be complete English sentences.

Avoid duplicating comments between `.h` and `.cpp`; document the public contract
in the header and keep implementation comments focused on non-obvious reasoning.

## PRs And Reviews

Before opening or reviewing a change, inspect:

- `CONTRIBUTING.md`
- `doc/coding-style.md`
- `.github/pull_request_template.md`
- Relevant `CMakeLists.txt` files and neighboring tests

Bolt PRs should be focused, include tests when behavior changes, describe
performance impact for core or hot-path changes, and call out breaking changes.

When posting review comments or CI analysis, draft the body first and get user
approval before submitting through GitHub or Codebase tools.

## Commit Messages

Use conventional commit-style titles such as:

```text
fix(parquet): Handle empty row groups
feat(exec): Add support for ...
test(functions): Cover ...
docs: Update ...
build: Adjust ...
ci: Run ...
```

Keep commits atomic. The body should explain the user-visible behavior or
problem, the core mechanism when it is not obvious, and the validation strategy
only when that strategy is not already clear from the diff.

## Common Mistakes

- Shipping bug fixes without a regression test.
- Updating tests to match a buggy result without first validating correctness.
- Treating behavior, naming, or build rules from another project as
  authoritative when Bolt has local documentation.
- Reconfiguring the whole build when a narrow existing CMake target is enough.
- Formatting files unrelated to the change.
- Making performance claims without reproducible benchmark data.
- Echoing or copying credentials from CI files, logs, or local configuration.

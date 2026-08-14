---
name: bolt-query
description: Answer questions about the Bolt codebase, architecture, implementation details, or pull requests. Use when the user asks how Bolt code works, asks for codebase analysis, or mentions /query.
---

# Bolt Query

Answer questions about Bolt code or PRs with local evidence from the repository.

## Context

- Bolt is a C++17 acceleration library focused on a reusable physical execution
  layer.
- Important areas include execution operators, expressions, functions, memory,
  connectors, storage readers and writers, serializers, shuffle, Substrait, and
  framework-specific Spark or Presto integration.
- Local Bolt files and documentation are authoritative.

## Workflow

1. Read `AGENTS.md`, `CONTRIBUTING.md`, and `doc/coding-style.md` when the
   answer depends on project conventions.
2. Explore the relevant source and tests before answering. Prefer `rg` and
   nearby `CMakeLists.txt` files to find ownership, targets, and test coverage.
3. If the question is about a PR or local diff, read the diff first, then inspect
   surrounding code and prior tests.
4. Reference exact file paths and line numbers when they materially support the
   answer.
5. Distinguish facts verified from code from assumptions or likely behavior.

## Areas To Check

- Public interfaces in headers and their implementation files.
- Neighboring tests under the closest `tests/` directory.
- Module-level `CMakeLists.txt` for target names and dependencies.
- Error handling via `BOLT_CHECK_*`, `BOLT_USER_CHECK_*`, `BOLT_FAIL()`,
  `BOLT_USER_FAIL()`, `BOLT_UNREACHABLE()`, and `BOLT_NYI()`.
- Performance-sensitive loops, allocation patterns, vector encodings, ownership,
  and lifetime assumptions.

## Output

Keep answers concrete. Start with the direct answer, then include supporting
evidence and caveats. Do not provide a broad architecture tour unless the user
asked for one.

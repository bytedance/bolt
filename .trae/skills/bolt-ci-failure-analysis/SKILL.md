---
name: bolt-ci-failure-analysis
description: Investigate failed Bolt GitHub Actions or internal Codebase CI runs. Use when the user asks why CI failed, asks to analyze logs, or mentions failed checks, workflow runs, or pipelines.
---

# Bolt CI Failure Analysis

Analyze Bolt CI failures from evidence, then explain the likely cause and next
action. Do not guess from job names alone.

## CI Surfaces

Bolt uses both GitHub Actions and internal Codebase pipelines.

GitHub workflows live in `.github/workflows/`. The main source workflow is
`build-test.yml`, which builds targets such as `benchmarks-build`,
`release_spark_with_test`, and `debug_spark_with_test`, runs clang-tidy for the
Spark release matrix, and runs `make ctest_release`.

Internal CI lives under `.codebase/pipelines/`. The main pipeline runs unit
tests, Gluten C++ tests, and Spark backend tests, with logs uploaded as
artifacts.

## Workflow

1. Identify the CI host, run ID or pipeline ID, failed job names, branch, commit,
   and target.
2. Fetch logs with the appropriate tool:
   - GitHub: `gh run view`, `gh api`, `gh pr checks`, and downloaded artifacts.
   - Codebase: use the `codebase` CLI and the `codebase-cli` skill.
3. Locate the first real failure in the build or test step. Ignore later cascade
   errors until the primary failure is understood.
4. Classify the failure as build, test, format, clang-tidy, benchmark,
   dependency, infra, or unknown.
5. For test failures, extract the test binary, suite, test name, assertion,
   source file, and line.
6. For build failures, extract the first compiler or linker error with file and
   line when available.
7. Compare the failure to the PR or local diff. State whether it appears related
   and why.
8. Check for evidence of known flakiness or main-branch failures when the failure
   does not line up with the diff.

## Reproduction Commands

Prefer narrow local reproductions:

```bash
cmake --build --preset conan-release --target <target>
ctest --test-dir _build/Release -R <regex> --output-on-failure
./_build/Release/path/to/test_binary --gtest_filter=Suite.Test
```

If the configured build type differs, switch the preset and `_build/<BuildType>`
directory accordingly.

For full CI-like checks:

```bash
make release_with_test
make unittest_release
make release_spark_with_test
make benchmarks-build
make clang-format-check
```

## Output

Use concise Markdown:

````markdown
## CI Failure Analysis

### Failed Job
- Job: ...
- Type: build | test | format | clang-tidy | benchmark | dependency | infra | unknown
- Failing target or test: ...

### Primary Failure
```text
<short relevant log excerpt>
```

### Correlation With Changes
...

### Reproduce Locally
```bash
...
```

### Recommended Next Step
...
````

If logs are missing or inaccessible, say exactly what evidence is missing and
what command or artifact would provide it.

## Rules

- Keep log excerpts small and relevant.
- Do not expose credentials, tokens, internal passwords, or full environment
  dumps from CI logs.
- Do not claim a failure is flaky unless there is supporting evidence.
- If many tests fail, explain the shared root cause if visible; otherwise show
  the first few representative failures and summarize the rest.

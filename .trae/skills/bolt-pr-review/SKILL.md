---
name: bolt-pr-review
description: Review Bolt pull requests, merge requests, or local diffs for correctness, memory safety, performance, compatibility, and test coverage. Use when the user asks for a review or mentions /pr-review.
---

# Bolt PR Review

Review Bolt changes with a focus on what CI cannot reliably prove:
correctness, memory safety, performance risk, compatibility, and test quality.

## Inputs

The user may provide a GitHub PR, a Codebase MR, a branch comparison, or ask for
a local worktree review. Use the available local and remote tooling for the
specific host:

- GitHub PRs: prefer `gh pr view`, `gh pr diff`, and `gh pr checks` when the
  user is working with GitHub.
- Internal Codebase MRs: prefer the `codebase` CLI and the `codebase-cli` skill
  when the user is working with Codebase.
- Local changes: use `git status --short --branch`, `git diff --stat`, and
  `git diff`.

Do not post review comments unless the user explicitly asks. If posting is
requested, draft the body first and ask for approval before calling remote APIs.

## Workflow

1. Read `AGENTS.md`, `CONTRIBUTING.md`, `doc/coding-style.md`, and
   `.github/pull_request_template.md`.
2. Collect the diff, changed-file list, commit history, PR or MR description,
   and prior review comments when available.
3. Identify the purpose and blast radius of the change before reading line by
   line.
4. Inspect neighboring code and tests for each changed area.
5. Check whether behavior changes include focused tests or a clear reason tests
   are not needed.
6. For hot-path or core-engine changes, check the performance section of the PR
   and ask for reproducible benchmark evidence if it is missing.

## Review Areas

| Area | Focus |
| --- | --- |
| Correctness | Edge cases, nulls, empty inputs, first and last rows, overflow, ordering, floating point corner cases, error states |
| Memory safety | Ownership, lifetimes, dangling references, buffer bounds, allocator use, exception safety, cleanup on partial failure |
| Concurrency | Data races, shared state, task cancellation, lock ordering, async callbacks, thread-local assumptions |
| Performance | Unnecessary copies, allocations in hot paths, vector encoding churn, cache-unfriendly loops, avoidable reconfiguration or I/O |
| Compatibility | Spark/Presto semantics, storage-format behavior, public API or ABI changes, config defaults, file format compatibility |
| Error handling | Correct `BOLT_CHECK_*` vs `BOLT_USER_CHECK_*`, useful messages, no swallowed failures |
| Tests | Regression tests for bug fixes, edge cases, failure paths, deterministic data, and appropriate module-local placement |

## Output Format

Lead with findings, ordered by severity. Use this shape:

```markdown
### Findings
- **Critical** [file:line]: ...
- **Major** [file:line]: ...
- **Minor** [file:line]: ...

### Open Questions
- ...

### Summary
One or two sentences about the reviewed scope.
```

If there are no issues, say that clearly and mention any residual risk or tests
you could not run. Do not pad a clean review with style trivia.

## Review Rules

- Do not flag formatting that `clang-format` or CI will handle unless the issue
  hides a real defect.
- Do not repeat prior review comments unless a new revision failed to address
  them.
- Avoid broad style preferences. Anchor every issue in behavior, maintainability,
  performance, or reviewer comprehension.
- For bug-fix PRs, flag missing regression tests unless the diff or PR explains
  why a test is impractical.
- Treat performance claims as unproven without commands, data, and methodology.

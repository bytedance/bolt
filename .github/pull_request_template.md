### What problem does this PR solve?
<!--
Please explain the context and the problem.
If this fixes a specific issue, please link it below.
-->
Issue Number: close #xxx

### Type of Change
<!-- Please check the one that applies to this PR -->
- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ✨ New feature (non-breaking change which adds functionality)
- [ ] 🚀 Performance improvement (optimization)
- [ ] ⚠️ Breaking change (fix or feature that would cause existing functionality to change)
- [ ] 🔨 Refactoring (no logic changes)
- [ ] 🔧 Build/CI or Infrastructure changes
- [ ] 📝 Documentation only

### Description

Describe your changes in detail.
For complex logic, explain the "Why" and "How".

### Performance Impact
<!--
REQUIRED for Performance PRs and Core Engine changes.
Please verify that your change does not introduce performance regressions.
-->
- [ ] **No Impact**: This change does not affect the critical path (e.g., build system, doc, error handling).
- [ ] **Positive Impact**: I have run benchmarks.
<details>
<summary>Click to view Benchmark Results</summary>

```text
Paste your google-benchmark or TPC-H results here.
Before: 10.5s
After:   8.2s  (+20%)
```

</details>

- [ ] **Negative Impact**: Explained below (e.g., trade-off for correctness).

### Release Note
<!-- Please write a short summary for the release notes. -->

Please describe the changes in this PR

Release Note:

```text
Release Note:
- Fixed a crash in `substr` when input is null.
- optimized `group by` performance by 20%.
```

### Checklist (For Author)
<!--
Please double-check the following before submitting.
-->

- [ ] I have added/updated unit tests (ctest).
- [ ] I have verified the code with local build (Release/Debug).
- [ ] I have run clang-format / linters.
- [ ] (Optional) I have run Sanitizers (ASAN/TSAN) locally for complex C++ changes.
- [ ] No need to test or manual test.

### Breaking Changes
<!--
Does this PR introduce API or ABI incompatibilities?
If yes, please describe how users should migrate.
-->

- [ ] No
- [ ] Yes (Description: ...)
<details>
<summary>Click to view Breaking Changes</summary>
```text
Breaking Changes:
- Description of the breaking change.
- Possible solutions or workarounds.
- Any other relevant information.
```
</details>  

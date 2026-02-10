@fzhedu I've addressed the review comments:

1. **Reverted the previous commit**: It wasn't addressing the actual root cause of the failures.
2. **Fixed Shadowing/Segfault**: In the `AsyncLoadHolder` constructor, the parameter `asyncThreadCtx` was shadowing the member variable. This meant we were likely dereferencing a moved-from parameter when calling `preloadBytesLimit()`. Renamed the parameter to `asyncThrCtx` to resolve this.
3. **Added Null Check**: Included an explicit check for `asyncThreadCtx` in the `AsyncLoadHolder` constructor.
4. **Restored Tests**: Recovered the `preloadingSplitClose` test in `TableScanTest.cpp` as requested.

Everything looks solid now. Ready for re-review!

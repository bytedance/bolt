# 05. 实施任务和测试

## Task 1：Task 级 BufferManager

**文件：**
- 修改：`bolt/exec/Task.h`
- 修改：`bolt/exec/Task.cpp`
- 修改：`bolt/exec/Driver.h`
- 修改：`bolt/exec/Driver.cpp`
- 测试：`bolt/exec/tests/TaskTest.cpp`

- [ ] 写默认关闭测试：不带 BM config 的 Task 不创建 BM。

```cpp
auto task = Task::create(..., std::nullopt);
ASSERT_EQ(nullptr, task->bufferManager());
```

- [ ] 写 Task BM identity 测试：开启 BM config 且提供 spillDiskOpts 时，Task 创建阶段已经持有 BM。

```cpp
auto bm1 = task->bufferManager();
auto bm2 = task->bufferManager();
ASSERT_NE(nullptr, bm1);
ASSERT_EQ(bm1.get(), bm2.get());

DriverCtx ctx(task, 0, 0, 0, 0);
ASSERT_EQ(bm1.get(), ctx.bufferManager().get());
```

- [ ] 写 fail-fast 测试：开启 BM config 但没有 spill directory 时，`Task::create()` 抛清晰错误。
- [ ] 在 `Task::init()` 里调用 `maybeCreateBufferManager()`，位置在 `initTaskPool()` 和 `setSpillDiskConfig()` 之后。
- [ ] 实现 Task getter，getter 只返回已创建 BM，不做懒初始化。
- [ ] 实现 DriverCtx getter，`task == nullptr` 时返回 nullptr。
- [ ] 构建并运行测试。

## Task 2：RowContainer RowId 基础能力

**文件：**
- 修改：`bolt/exec/RowContainer.h`
- 修改：`bolt/exec/RowContainer.cpp`
- 测试：`bolt/exec/tests/RowContainerTest.cpp`

- [ ] 写 RowId 编解码测试：

```cpp
auto rowId = RowContainer::testingMakeRowId(7, 4096);
ASSERT_EQ(7, RowContainer::testingRowIdBlock(rowId));
ASSERT_EQ(4096, RowContainer::testingRowIdOffset(rowId));
```

- [ ] 添加 `makeRowId()`、`rowIdBlock()`、`rowIdOffset()`。
- [ ] 确认默认构造行为不变。

## Task 3：BM-backed append-only rows/string block

**文件：**
- 修改：`bolt/exec/RowContainer.h`
- 修改：`bolt/exec/RowContainer.cpp`
- 测试：`bolt/exec/tests/RowContainerTest.cpp`

- [ ] 写 BM append-only round-trip 测试，覆盖 BIGINT、VARCHAR、ARRAY(VARCHAR)、小 block 多 block、`clear()`。
- [ ] 扩展 `RowContainerParam`。
- [ ] 实现 rows block 分配。
- [ ] 实现 string block 分配，或第一阶段明确保留 `stringBuffers_`。
- [ ] 实现 `allocatedBytes()`、`usedBytes()`、`freeSpace()`、`estimateRowSize()`、`sizeIncrement()` 的 BM 分支。

## Task 4：RowContainer pinRows/eraseRowIds

**文件：**
- 修改：`bolt/exec/RowContainer.h`
- 修改：`bolt/exec/RowContainer.cpp`
- 测试：`bolt/exec/tests/RowContainerTest.cpp`

- [ ] 写 pin after spill 测试：
  - BM append-only RowContainer 写入多行。
  - 保存 RowId。
  - 释放封口 block append pin。
  - 调 `bufferManager->SpillBlocks()` 或 `Reclaim()`。
  - `pinRows()` 重新读取。
  - 验证行内容正确。
- [ ] 实现 `PinnedRows`。
- [ ] 实现 `pinRows()`。
- [ ] 实现 `eraseRowIds()`。

## Task 5：WindowBuild 传入 BM append-only options

**文件：**
- 修改：`bolt/exec/WindowBuild.h`
- 修改：`bolt/exec/WindowBuild.cpp`
- 修改：`bolt/exec/Window.cpp`
- 修改：`bolt/core/QueryConfig.h`

- [ ] 增加默认关闭 QueryConfig：`buffer-manager-enabled`。
- [ ] WindowBuild 支持 row container options。
- [ ] Window.cpp 只在 `StreamingWindowBuild && !needSort_` 传 BM。
- [ ] 验证默认路径不变。

## Task 6：StreamingWindowBuild 使用 RowId

**文件：**
- 修改：`bolt/exec/StreamingWindowBuild.h`
- 修改：`bolt/exec/StreamingWindowBuild.cpp`
- 测试：Window 相关测试

- [ ] 写 BM StreamingWindowBuild 输出一致性测试，覆盖多 partition、跨 block、VARCHAR/ARRAY(VARCHAR)。
- [ ] 新增 BM 分支成员 `sortedRowIds_`、`inputRowIds_`、`previousRowId_`。
- [ ] `addInput()` 使用 `newRowId()` + `pinRows()`。
- [ ] `nextPartition()` 返回 `RowIdWindowPartition`。
- [ ] erase 旧 partition 用 `eraseRowIds()`。

## Task 7：RowIdWindowPartition lazy pin

**文件：**
- 修改：`bolt/exec/WindowPartition.h`
- 修改：`bolt/exec/WindowPartition.cpp`
- 测试：Window 相关测试

- [ ] 实现 `RowIdWindowPartition`。
- [ ] `extractColumn()` 按调用范围 pin。
- [ ] `extractNulls()`、peer、frame 按方法范围 pin。
- [ ] 用小 block 强制跨 block 测试。

## Task 8：构建依赖和观测

**文件：**
- 修改：`bolt/exec/CMakeLists.txt`
- 可能修改：stats/log 相关文件

- [ ] `bolt_exec` 链接 `bolt_memory_bm`。
- [ ] BM 模式开启时记录 task id、operator id、block size、block 数、BM debugString。
- [ ] 构建 `bolt_exec` 和 `bolt_exec_test`。

## 测试矩阵

### RowContainer

- 默认非 append-only 现有测试。
- append-only 非 BM 现有测试。
- BM append-only fixed-width。
- BM append-only VARCHAR 非 inline。
- BM append-only ARRAY/MAP/ROW。
- BM append-only 多 rows block。
- BM append-only 多 string block。
- `pinRows()` 输入跨多个 block。
- block unpin -> spill/reclaim -> pinRows -> 内容正确。
- `eraseRowIds()` 后 live row 计数和 used/allocated bytes 合理。

### Window

- 默认 StreamingWindowBuild 输出不变。
- BM StreamingWindowBuild 单 partition 单 block。
- BM StreamingWindowBuild 单 partition 多 block。
- BM StreamingWindowBuild 多 partition。
- BM StreamingWindowBuild partition erase 后继续处理后续 partition。
- BM StreamingWindowBuild 包含 VARCHAR/ARRAY(VARCHAR)。
- `extractColumn()`、`extractNulls()`、`computePeerBuffers()`、range frame bound 覆盖。

## 验证命令

查看当前构建类型：

```bash
cat _build/.build_type
```

构建 exec：

```bash
cmake --build --preset conan-release --target bolt_exec
```

构建 exec tests：

```bash
cmake --build --preset conan-release --target bolt_exec_test
```

跑 RowContainer 测试：

```bash
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*RowContainer*'
```

跑 Window 测试：

```bash
_build/Release/bolt/exec/tests/bolt_exec_test --gtest_filter='*Window*'
```

跑 BM 测试：

```bash
_build/Release/bolt/common/memory/bm/tests/bolt_memory_bm_test
```

# StreamingWindowBuild 下 RowContainer 接入 BufferManager 方案索引

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**目标：** 基于提交 `0f6b42a9cf0001f91fab051dbe4dfbd00ad747a4` 已引入的 RowContainer append-only 路径，将 `StreamingWindowBuild` 场景中的 RowContainer 迁移到 Task 级 `BufferManager` 管理，并用稳定 RowId 替代长期保存的裸 `char*`。

**架构：** `Task` 持有执行期 `BufferManager`；`RowContainer` 增加 BM-backed append-only 后端；`StreamingWindowBuild` 长期保存 RowId；`WindowPartition` 按调用范围 lazy pin。

**技术栈：** C++20、Bolt exec、`bolt/common/memory/bm`、`RowContainer`、`StreamingWindowBuild`、`WindowPartition`、gtest、CMake。

---

## 阅读顺序

1. [00-overview.md](rowcontainer-bm-streaming-window/00-overview.md)
   - 总体目标、基线 commit、拆分后的设计主线。

2. [01-current-state-and-invariants.md](rowcontainer-bm-streaming-window/01-current-state-and-invariants.md)
   - 当前代码路径、append-only 现状、必须遵守的不变量。

3. [02-task-buffer-manager-and-config.md](rowcontainer-bm-streaming-window/02-task-buffer-manager-and-config.md)
   - Task 级 BM 生命周期、DriverCtx 暴露、QueryConfig 开关。

4. [03-rowcontainer-bm-appendonly.md](rowcontainer-bm-streaming-window/03-rowcontainer-bm-appendonly.md)
   - RowId、BM-backed append-only block、`pinRows()`、`eraseRowIds()`。

5. [04-streaming-window-and-partition.md](rowcontainer-bm-streaming-window/04-streaming-window-and-partition.md)
   - `WindowBuild` 接入、`StreamingWindowBuild` RowId 化、`RowIdWindowPartition` lazy pin。

6. [05-implementation-tasks-and-tests.md](rowcontainer-bm-streaming-window/05-implementation-tasks-and-tests.md)
   - 分阶段任务、测试矩阵、构建验证命令。

7. [06-risks-and-scope.md](rowcontainer-bm-streaming-window/06-risks-and-scope.md)
   - 主要风险、第一阶段范围、建议提交拆分。

8. [07-chunked-frame-roadmap.md](rowcontainer-bm-streaming-window/07-chunked-frame-roadmap.md)
   - 为解决单个 frame 放不下内存而设计的分阶段路线、覆盖率和实现边界。

## 推荐第一阶段范围

第一阶段只做端到端最小闭环：

- Task 级 BM getter。
- RowContainer BM-backed fixed row append-only block。
- RowId + `pinRows()`。
- `StreamingWindowBuild` BM 分支用 RowId。
- `RowIdWindowPartition` 按方法 lazy pin。
- QueryConfig 默认关闭。
- 输出一致性测试。

第一阶段不做：

- HashTable/Aggregation/SortBuffer/TopN 迁移。
- SortWindowBuild/SpillableWindowBuild 迁移。
- Row slot reuse。
- generation 编码。
- 全局替换所有 `char*` RowContainer API。

## 分块 frame 路线

针对“单个 frame 放不下内存”的问题，单独见：

[07-chunked-frame-roadmap.md](rowcontainer-bm-streaming-window/07-chunked-frame-roadmap.md)

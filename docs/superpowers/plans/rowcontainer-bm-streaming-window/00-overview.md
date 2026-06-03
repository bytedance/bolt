# 00. 总览

## 基线

本方案基于当前 HEAD：

```text
0f6b42a9cf0001f91fab051dbe4dfbd00ad747a4 Add appendOnly path in RowContainer
```

该提交已经完成了关键前置工作：

- `RowContainerParam` 新增 `appendOnly`。
- `RowContainer` 新增 `appendOnly_`。
- append-only 下 `newRow()` 走 `rowsBuffers_`。
- append-only 下 VARCHAR/VARBINARY 走 `stringBuffers_`。
- append-only 下复杂类型序列化后复制到 `stringBuffers_`。
- append-only 下 `freeVariableWidthFields()` 变成整块生命周期释放。
- `StringView`/`StringViewBase` 已有 `offset()` 相关接口，为后续 offset 编码留下空间。

## 目标

把 `StreamingWindowBuild` 场景里的 RowContainer 存储迁移到 BM 机制，同时保持默认路径不变：

- BM 由 `Task` 持有。
- RowContainer 在 append-only 模式下可选使用 BM block。
- 上层不再长期保存 `char*`，改为保存 RowId。
- `WindowPartition` 不全量 pin partition，而是按方法调用范围 pin。

## 总体架构

```text
Task
  └── execution BufferManager
        └── RowContainer BM append-only backend
              ├── rows blocks: fixed row payload
              └── string blocks: variable payload, 可分阶段迁移

StreamingWindowBuild
  └── sorted/input rows: RowId
        └── RowIdWindowPartition
              └── pinRows(rowIds) -> temporary char* views
```

## 最重要的设计边界

`BlockHandle` 是长期 block 身份，`BufferHandle` 是一次 pin，`char*` 只是 pin 生命周期内的短期地址。

因此：

- 长期保存：RowId。
- 短期访问：`PinnedRows` 里的 `char*`。
- 释放 pin 后：不能继续使用这些 `char*`。


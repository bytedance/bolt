# 06. 风险和范围

## 主要风险

### stale char*

最大风险是某个路径仍长期保存 `char*`。迁移时需要重点搜索：

```bash
grep -RIn "std::vector<char\\*>\\|char\\* previous\\|folly::Range<char\\*\\*>" bolt/exec/StreamingWindowBuild.* bolt/exec/WindowPartition.*
```

BM 模式下这些成员级缓存都应改为 RowId 或由 RAII pin holder 持有。

### append-only 当前实现未考虑 BM 地址变化

当前 `rowsBuffers_`/`stringBuffers_` 地址稳定；BM block unpin 后再 pin 地址可能变化。所有 StringView/complex string_view 如果内部保存绝对地址，会受影响。

保守路线：

1. fixed row BM + RowId。
2. WindowPartition lazy pin fixed row。
3. stringBuffers 迁到 BM，并保证 string payload pin 生命周期。

如果一次性迁移 string block，则需要确保变长字段内部保存的是可在 pin 后重建的地址，或者 string block 在 RowPartition 访问期间也被 pin。`StringView::offset()` 和 `StringViewBase::offset()` 暗示后续可以走 offset 编码，但当前 RowContainer append-only 仍在写绝对地址。

### WindowPartition 方法可能隐式缓存 row

需要确认 `WindowFunction` 是否持有从 partition 返回的 row 指针。如果有，必须把 pin 生命周期扩大到对应调用完成。

### Task BM spill path

没有合法 spill directory 时，BM 创建可能失败。配置必须明确：

- 默认关闭 BM RowContainer。
- 开启但缺少 spill path 时给清晰错误。

### 链接依赖扩大

`bolt_exec` 链接 `bolt_memory_bm` 会引入 bm file/io/compress 依赖。需要通过 `bolt_exec` 和 `bolt_exec_test` 构建验证。

## 第一阶段推荐范围

第一阶段完成：

- Task 级 BM getter。
- RowContainer BM-backed fixed row append-only block。
- RowId + `pinRows()`。
- StreamingWindowBuild BM 分支用 RowId。
- RowIdWindowPartition 按方法 lazy pin。
- QueryConfig 默认关闭。
- 输出一致性测试。

第一阶段不做：

- HashTable/Aggregation/SortBuffer/TopN 迁移。
- SortWindowBuild/SpillableWindowBuild 迁移。
- Row slot reuse。
- generation 编码。
- 全局替换所有 `char*` RowContainer API。

string block 是否第一阶段一起迁移取决于对 `StringView` 绝对地址风险的验证结果。保守路线是先迁 fixed row，再单独迁 string block。

## 建议提交拆分

1. `exec: expose task execution buffer manager`
2. `exec: add rowcontainer row ids for append-only storage`
3. `exec: back append-only row buffers by buffer manager`
4. `exec: add rowcontainer pinned rows api`
5. `exec: add rowid window partition`
6. `exec: use row ids in streaming window build bm path`
7. `exec: gate streaming window bm rowcontainer by config`
8. `exec: add bm streaming window tests`


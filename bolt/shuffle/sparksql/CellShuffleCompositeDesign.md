# CellShuffle CompositeRowVector 支持方案

状态：设计，尚未实现。当前开启 aggregation composite output 的计划继续回退 V1。
本期不修改 CompositeRowVector、GroupingSet 或聚合状态 serde API。

## 数据语义

CompositeRowVector 的 rows / rawRows 并非普通 SQL CompactRow：GroupingSet 使用
`ContainerRow2RowSerde` 保存聚合内部状态，最终聚合依赖 RowFormatInfo 解释状态并
修复内部指针。不得当成 SQL 复杂列再次 CompactRow 编解码，也不得先将整批转成列。

支持范围包括输入和 reader 输出。保留原始 state row bytes，经 Cell 的二进制
存储、压缩和传输，再恢复 CompositeRowVector；逻辑 schema、有效 children 和
聚合状态解释信息由外部上下文传递并校验一致。

## 独立接口

- `CellInputAdapter`：普通 RowVector 与 Composite 各自实现输入视图。普通复杂列
  继续使用现有 CellShuffleTypeAdapter；Composite 实现直接返回 pid + state bytes。
- `CompositeRowView`：显式给出每行的 byte span、length、offset、alignment，以及
  保持底层 row buffer 存活的 owner。由 producer 生成，不允许 reader 猜测固定
  4 字节长度前缀，或访问 invalid children。
- `CellBatchDecoder`：普通输出使用逻辑类型 adapter，Composite 输出使用单独的
  `CompositeBatchDecoder`。核心 Cell 字段循环只处理声明的物理列。
- runtime factory 在边界装配上述组件；output / write policy 与输入表示正交，
  将来 Local 和 Celeborn 可以共用 Composite adapter。

现有 CompositeRowVector 有 rowOffset、alignment、rows、rawRows、rowBuffer、
rowToBufferMapping，但没有足够明确的逐行受限 span API。落地时先补 producer 与
vector 的这个契约，再接入 Cell，不能从当前指针布局推断所有行的长度。

## 线格式与生命周期

需要外部协商的 layout discriminator（或未来 payloadKind），区分 ordinary 与
aggregate-state。若同一 shuffle 允许混合输入，只在完整窗口边界切换，并为每个
窗口携带可验证的 layout。不能通过 Binary 列内容、列名或首个 batch 猜测。
这个扩展需要单独的格式修订及 writer / reader / reference 同步实现；本期不加入。

Writer 在对应单元提交完成前保留源 owner。Reader 解码 state bytes 后，根据明确
的 offset / alignment 布局构造稳定存储，设置 rows / rawRows 和有效 children；
允许必要的一次 owner buffer copy，不要求整批转列。恢复输出不得依赖临时 payload
工作区；最终聚合继续由 ContainerRow2RowSerde 解释状态。

所有长度、总量、对齐计算先检查越界/溢出。单行超出允许的 payload 或可用预算时
在写入前报错，不跨 payload 分片单行。失败和取消释放所有 row owners。

## 验证与落地顺序

1. Row span API 与 producer 契约测试：有无 prefix、非零 offset、alignment、空行、
   多 buffer、切片、引用计数、invalid children。
2. Composite input adapter 与 state bytes 单窗口字节一致性。
3. Reader 输出 Composite 的生命周期、RowFormatInfo、最终聚合结果一致性。
4. 多窗口、混合输入边界、内存回收、压缩、超大单行和损坏数据。
5. 在 Local 验证后，再与独立的 Celeborn output / policy 做组合测试。

启用条件必须由计划和配置静态决定，Reader 与 Writer 对称；完成上述验证前保留
现有 V1 回退，不能只打开 writer 而让 reader 返回普通 RowVector。

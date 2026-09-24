# CellShuffle Celeborn 支持方案

状态：设计，尚未实现。当前 Celeborn 继续回退 V1。本期只实现 Local 普通
RowVector 全类型支持；复杂类型已经在 Cell 入口前变成末尾 Binary，不影响远端接口。

## 模块边界

- `CellPayloadCodec`：从完整分区窗口构造 ColumnarPayload，负责编码、压缩、大小
  计算。由现有 LocalCellOutput 的公共序列化部分提取，不包含文件或 RSS 调用。
- `CellOutput`：输出完整窗口及反馈已释放字节。Local 实现持有本地 spill / merge；
  Celeborn 实现只持有发送队列、SDK client 和完成状态。
- `CellWritePolicy`：Local 保留现有分配途中 spill；Remote 在安全边界做预算准入、
  选择待发送分区和回压。Cell 的类型分派循环不检查 partition writer kind。
- `CellRuntimeFactory`：根据静态配置选择 output / policy。Writer 与 Reader 必须
  使用一致的选择条件，禁止由输入值或内存压力触发不可见的格式切换。

本期不增加这些接口的空实现，也不修改 Celeborn SDK。

## 远端内存及推进协议

硬约束：Celeborn 路径不得创建或写入本地 spill 文件。

1. 每次最多处理 1024 行的输入单元。追加前估算最坏情况：新 cell、目录、null
   bitmap、字典 framing、合并/压缩工作区和发送缓冲。准入由任务 memory pool 保证。
2. 单元追加期间不允许 reclaim 观察半行或半提交的窗口。reclaim 只记录请求；
   在完整单元提交后执行。编码块、字典片段不得在 push 中间被任意截断。
3. 预算不足时，先从已提交分区选择占用最大的分区，完成编码，push 后释放该分区。
   保留编码所需的工作区预算；不能依靠一次临时大分配才能完成回收。
4. 默认最多 1 个 in-flight push。建议初始 `maxRemotePayloadBytes = 40 MiB`，
   结合最坏情况编码开销确定窗口上界；阈值属于远端 policy，不能改变 Local 行为。
5. 单行超过 payload 上限或无法在预算内完成时，在追加前明确报错。不跨 push
   分片单行，不通过本地落盘绕过失败。

## SDK 与所有权

现有 `RssClient::pushPartitionData` 返回字节数，不能视作 ACK。当前 native client
向 SDK 的 pushData 传递裸指针，SDK 复制到含 transport header 的 ByteBuffer，
并在该复制之后检查 in-flight 限额。现有 stop / mapperEnd 才等待全部请求完成，
缺少公开的非终止 drain 接口。因此直接复用当前 push API 不能满足上述预算保证。

SDK 扩展需支持：

- 发送前 admission / drain；可重复调用 drain，不结束 mapper。
- 引用计数、pool-owned 的发送 buffer（例如 IOBuf 的所有权回调），异步完成前
  不能回收；重试继续持有同一个所有者。完成回调释放 reservation 并唤醒等待者。
- 完成、错误、取消均可观察；取消后等回调/请求彻底退出才能销毁 buffer 和 pool。
- stop 顺序为完成当前窗口、等待 push、mapperEnd；mapperEnd 最多一次。失败不得
  将未确认的数据计作成功，析构不能再访问已释放的 query pool。

## Reader、指标与验证

Reader 按已有 ColumnarPayload 边界串联远端数据，codec 与 schema 由外部配置确定。
保留现有 client 返回值的 transport-byte 指标契约，另列 payload bytes、in-flight
bytes、等待时间，避免把复制完成或入队当成服务端确认。

测试须覆盖：禁止本地文件访问、极低预算、迟到 ACK、失败/重试/取消、stop 一次性、
空分区、超大单行、多个窗口、codec、复杂 Binary、UNKNOWN-only，以及 SDK 内存
与任务 pool 的归属。实现与 Celeborn 端到端验证完成前保持 V1 回退。

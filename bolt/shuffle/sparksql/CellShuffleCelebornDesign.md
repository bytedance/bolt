# CellShuffle Celeborn 支持方案

状态：设计，尚未实现。当前 Celeborn 继续回退 V1。本期只实现 Local 普通
RowVector 的 Cell 类型支持，不含 VARIANT（包括嵌套 VARIANT）；选中 Cell 路径后，
不支持的类型在 Node adapter 入口报错，不按类型回退。Celeborn / Composite 的配置
回退保留。支持的复杂类型已经在 Cell 入口前变成末尾 Binary，不影响远端接口。

Celeborn 第一版采用全量 flush：内存预算不足时，在完整输入单元边界关闭所有分区
的当前窗口，逐分区发送，然后整体重置。暂不实现最大分区选择、局部窗口回收或
新的 freelist 机制。现有 allocator 的 freelist 保留供已有代码使用；远端全量
重置复用 `DataCells::reset()` 和 `ChunkAllocator::resetAll()`。

## 模块边界

当前 Local 写端由 Writer 拥有 allocator、output 和 splitter，先构造 output，再构造
借用 `CellOutput&` 的 splitter。Writer 负责 decode、分区、字典 probe 选择和预算
调度；splitter 以值成员持有 DataCells / NullCells，并拥有窗口行数、encoding tags
和变量字节。窗口生命周期入口为 `split()`、`spillRun()`、`sealWindow()` 和
`finish(metrics)`；`flushAll()` 和 `resetWindow()` 是 splitter 的私有操作。`CellWindowInput`
仅在 splitter 同步调用 output 时借用，不新增窗口类或额外缓冲层。

Local 的 `spillRun()` 只排出已链接的 Cell，关窗收尾期间允许 grow 回调触发 spill，
实际调用 output 时防止重入；`finish()` 保留 resident 数据直接输出的路径。
当前 `sealWindow()` 仍按 Local 的 run spill / seal 协议工作，不能直接视作远端
完整窗口 flush。接入 Celeborn 仍需适配 output 协议与预算策略：

- `CellPayloadCodec`：从完整分区窗口构造 ColumnarPayload，负责编码、压缩、大小
  计算。由现有 LocalCellOutput 的公共序列化部分提取，不包含文件或 RSS 调用。
- `CellOutput`：未来需适配完整窗口输出。Local 实现保留本地 spill / merge；Celeborn
  实现逐分区编码并发送，只持有有界发送缓冲、SDK client 和完成状态。splitter 在
  输出成功后统一重置 Cell 和窗口状态；Writer / policy 统计实际归还 pool 的字节，
  不把发送字节当作回收字节。同步借用视图不能留给异步发送使用。
- `CellWritePolicy`：Local 保留现有分配途中 spill；Remote 在安全边界做预算准入、
  全量 flush 和回压。未来通过 splitter 的完整窗口操作协调内部收尾、输出和重置，
  不公开调用私有 `flushAll()` / `resetWindow()`，也不增加按分区 flush / reset 接口。
  Cell 的类型分派循环不检查 partition writer kind。
- `CellRuntimeFactory`：根据静态配置选择 output / policy。Writer 与 Reader 必须
  使用一致的选择条件，禁止由输入值或内存压力触发不可见的格式切换。

本文仅定义下一阶段方案，不增加接口空实现；实际启用仍需完成下面的 SDK 能力。

## 远端内存及推进协议

硬约束：Celeborn 路径不得创建或写入本地 spill 文件。

1. 每次最多处理 1024 行的输入单元，并按字节预算缩小，必要时缩到单行。准入覆盖
   入口复杂类型 Binary、新 cell、目录、null bitmap，以及完整 flush 所需的 cache
   尾部、字典 framing、单分区编码/压缩工作区和发送缓冲。发送工作区提前预留，
   不能等实际分配失败后再申请；已有窗口加上新单元必须满足单分区 payload 上限。
2. 单元追加期间不允许 reclaim 观察半行或半提交的窗口。reclaim 只记录请求；
   在完整单元提交后执行，并如实报告当次实际回收量。追加中可能持有尚未链接的 cell
   id，不能在 `beforeGrow` 或 append 中途调用全量 flush / `resetAll()`。准入必须
   保证单元和 cache 收尾可以完成，flush 期间也禁止重入。
3. 下一单元无法准入、窗口达到限制或安全边界收到 reclaim 请求时，执行全量 flush：
   splitter 在内部收尾所有 cache 和字典片段，再通过适配后的 output 协议按 pid
   顺序处理所有 `rowCount > 0` 的分区，包括没有数据 stream 的 UNKNOWN-only 分区。
   此操作需要未来的远端窗口协议，不能直接复用当前 Local `sealWindow()` 的语义。
4. 每个分区编码为一个完整 ColumnarPayload，发送并等待完成及发送缓冲释放，再
   处理下一个分区。最多 1 个 in-flight push；不一次性物化所有分区的 payload。
   Cell / null 数据在本轮全部发送成功前保留，峰值预算包含这部分常驻内存。
5. 全部成功后，由 splitter 统一清空 DataCells 目录、`allocator.resetAll()`、重置
   nulls、窗口状态及行数计数。因内存压力触发时再由 Writer / policy 调用
   `allocator.shrink()` 并归还 pool 的 reservation；仅因窗口大小触发时可保留 chunk
   复用。整体 reset 不逐 cell 回收
   或排序 freelist；固定目录和 splitter cache 仍常驻，不承诺回收全部 writer 内存。
6. flush 成功后重新尝试输入准入；仍放不下则继续缩小单元，单行无法容纳时明确
   报错。不通过本地落盘绕过失败。部分分区发送成功后发生失败，终止该 map attempt，
   不重新发送整轮窗口；请求重试及 batch identity 由 SDK 管理。

`maxRemotePayloadBytes` 限制单次 push 中的 Cell payload 字节，不含 Celeborn header；
header 和 SDK 工作内存另外计入预算。40 MiB 仅作为候选初值，单行上限须与入口
adapter 的 64 MiB 限制协调，并按包含 framing 的保守上界在追加前检查。每个 payload
独立解码，不跨 push 拆分。这些阈值属于远端 policy，不改变 Local 行为。

## SDK 与所有权

现有 `RssClient::pushPartitionData` 返回字节数，不能视作 ACK。当前 native client
向 SDK 的 pushData 传递裸指针，SDK 复制到含 transport header 的 ByteBuffer，
并在该复制之后检查 in-flight 限额。现有 stop / mapperEnd 才等待全部请求完成，
缺少公开的非终止 drain 接口。因此直接复用当前 push API 不能满足上述预算保证。

SDK 扩展需支持：

- 发送前 admission / drain；可重复调用 drain，不结束 mapper。第一版允许用
  push 后等待完成的同步封装，不能用 `stop()` 代替每轮 drain。
- 发送 buffer 的所有权和预算覆盖整个异步请求及重试生命周期。第一版允许受控
  复制，双份缓冲都需计入预算；零拷贝不是启用前提。仅在所有使用者释放 buffer
  后归还对应 reservation，不能仅根据 push 返回或 ACK 到达提前回收。
- 完成、错误、取消均可观察；取消后等回调/请求彻底退出才能销毁 buffer 和 pool。
- stop 顺序为全量 flush、等待 push、mapperEnd；空 mapper 也执行一次 mapperEnd。
  失败走独立的 abort / cleanup，不将未确认的数据计作成功；析构不能再访问已释放
  的 query pool。

## Reader、指标与验证

Reader 按已有 ColumnarPayload 边界串联远端数据，codec 与 schema 由外部配置确定。
Celeborn reader 使用 `needCompression=false`，由 Cell decoder 处理 payload 内部压缩。
保留现有 client 返回值的 transport-byte 指标契约，另列 payload bytes、in-flight
bytes、等待时间，避免把复制完成或入队当成服务端确认。

测试须覆盖：禁止本地文件访问、极低预算、追加中 reclaim 延迟、flush 禁止重入、
多分区共享 chunk 的整体释放、flush 后继续追加、cache 尾部及字典收尾、逐分区发送
的内存峰值、迟到 ACK、部分发送失败/重试/取消、stop 一次性、空 mapper / 空分区、
超大单行、多个窗口、codec、复杂 Binary、UNKNOWN-only，以及 SDK 内存与任务 pool
的归属。实现与 Celeborn 端到端验证完成前保持 V1 回退。

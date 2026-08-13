# DuckDB算子结合BufferManager机制

## **Sort 如何结合 BufferManager**

Sort 的核心类是 `Sort` 和 `SortedRun`。这里要先区分两个层次：

```Plain Text
Sort 算子层面的 external:
    决定一个 local SortedRun 是否要提前 finalize 成 run，
    后续由 SortedRunMerger 做 merge。

BufferManager 层面的 spill:
    某个 TupleDataCollection block unpin 后进入 eviction queue，
    内存压力下被写入 temp 文件。
```

Sort 本身不做 radix partition，也不直接写 spill 文件。它做的是 **run 切分**：当当前线程的 \`SortedRun\` 超过 \`TemporaryMemoryState\` 给的预算时，把这个 run 排序并交给全局 \`sorted\_runs\`。这些 run 内部的数据 block 何时真正落盘，由 BufferManager 的 eviction 决定。



Sink 路径：

```Plain Text
Sort::GetGlobalSinkState
    |
    +-- TemporaryMemoryManager::Register()
    +-- external 初始值来自 debug_force_external

Sort::Sink(input chunk)
    |
    +-- 如果 local state 还没有 SortedRun，创建一个
    +-- 执行 create_sort_key expression
    +-- payload 引用投影列
    +-- SortedRun::Sink(key, payload)
          |
          +-- key_data->Append(...)
          +-- payload_data->Append(...)
          +-- row/heap blocks 通过 BufferManager 分配
    |
    +-- 如果 local run size 超过 local reservation:
          |
          +-- 尝试提高 TemporaryMemoryState reservation
          +-- 如果 reservation 仍然不够，标记 external
          +-- finalize 当前 run，加入 global sorted_runs
```

对应伪代码可以写成：

```Plain Text
SortGlobalSinkState:
    num_threads = TaskScheduler::NumberOfThreads()
    temp_state = TemporaryMemoryManager.Register(context)
    external = debug_force_external
    sorted_runs = []

SortLocalSinkState:
    sorted_run = null
    maximum_run_size = 0
    external = false

Sort::Sink(chunk):
    if local.sorted_run == null:
        local.sorted_run = new SortedRun(context, sort)
        local.maximum_run_size = global.temp_state.reservation / global.num_threads
        local.external = global.external

    key = create_sort_key(chunk)
    payload = project_payload_columns(chunk)
    local.sorted_run.Sink(key, payload)

    if local.sorted_run.SizeInBytes() < local.maximum_run_size:
        return NEED_MORE_INPUT

    if local.external:
        local.sorted_run.Finalize(external = true)
        global.sorted_runs.push(local.sorted_run)
        local.sorted_run = null
        return NEED_MORE_INPUT

    lock(global)
    refresh local.maximum_run_size/local.external from global

    if local.sorted_run.SizeInBytes() < local.maximum_run_size:
        unlock(global)
        return NEED_MORE_INPUT

    if global.temp_state.reservation < global.temp_state.remaining_size:
        if !global.any_combined:
            global.external = true
    else:
        required = global.num_threads * local.sorted_run.SizeInBytes()
        if is_index_sort:
            required *= 4

        request = global.temp_state.remaining_size * 2
        while request < required:
            request *= 2

        global.temp_state.SetRemainingSizeAndUpdateReservation(request)

        if global.temp_state.reservation < required and !global.any_combined:
            global.external = true

    refresh local.maximum_run_size/local.external
    unlock(global)

    if local.external and local.sorted_run.SizeInBytes() >= local.maximum_run_size:
        local.sorted_run.Finalize(external = true)
        global.sorted_runs.push(local.sorted_run)
        local.sorted_run = null

    return NEED_MORE_INPUT
```

这里的几个判断点很重要：

```Plain Text
什么时候切 run?
    当前 local SortedRun 的 SizeInBytes() >= local.maximum_run_size，
    且 local.external == true。

什么时候进入 external sort?
    1. debug_force_external 打开；
    2. 或者 TemporaryMemoryManager 给的 reservation 小于 previously requested remaining_size；
    3. 或者当前 run 所需 required 内存申请后仍拿不到。

什么时候真正 spill 到磁盘?
    Sort::Sink/Finalize 不直接写磁盘。
    SortedRun 里的 TupleDataCollection block 被 unpin 后，
    BufferPool 后续因为内存压力 evict 它时才 WriteTemporaryBuffer。
```

Sort 的数据布局：

```Plain Text
input chunk
    |
    +-- create_sort_key(...)
    |        |
    |        v
    |    key TupleDataCollection  -- BufferManager blocks
    |
    +-- payload columns
             |
             v
         payload TupleDataCollection -- BufferManager blocks
```



`SortedRun::Finalize(external)` 会 finalize append pin state。之后：



- 如果 `external == false`，它要求数据仍然 pinned，然后在内存里排序。

- 如果 `external == true`，它会把排序后的数据组织成适合外部 merge 的 collection，并使用 `UNPIN_AFTER_DONE` 释放旧 block，使 BufferManager 有机会 evict/spill。

更细一点看，`SortedRun` 的 block 生命周期大致是：

```Plain Text
SortedRun::Sink
    |
    +-- key_data.Append(...)
    +-- payload_data.Append(...)
    +-- append pin property 初始是 KEEP_EVERYTHING_PINNED
    |
    v
SortedRun::Finalize(external)
    |
    +-- key_data.FinalizePinState(...)
    +-- payload_data.FinalizePinState(...)
    |
    +-- external == false:
    |       VerifyEverythingPinned()
    |       在 pinned block 上排序
    |
    +-- external == true:
            排序/重排过程中使用 UNPIN_AFTER_DONE
            已处理 block 释放 pin
            后续 BufferManager 可按需 evict 到 temp 文件
```

Source 路径：

```Plain Text
Sort::Finalize
    |
    +-- 所有 sorted_runs ready

Sort::GetGlobalSourceState
    |
    +-- SortedRunMerger(sorted_runs, partition_size, external)

Sort::GetData
    |
    +-- SortedRunMerger::GetData
    +-- 按需 pin/read row blocks
    +-- decode sort key
    +-- gather payload columns
```

结论：Sort 自己不直接管理 spill 文件。它负责决定 run 边界和 external 模式；实际 block 的 unload、写临时文件、reload 都由 `TupleDataCollection` \+ BufferManager 完成。

一个容易误解的点：

```Plain Text
Sort external != 立刻写磁盘

Sort external 只是让 run 不再要求所有数据一直 pinned，
并允许后续 merge 阶段按需 pin/read。
磁盘写入只有在 BufferPool 真的选择 evict 某个 unloaded-eligible block 时发生。
```



## **Hash Join 如何结合 BufferManager**



Hash Join 有两类需要外部化的数据：



- build 侧：`JoinHashTable::sink_collection`，类型是 `RadixPartitionedTupleData`。

- probe 侧：external join 下暂时不能 probe 的行，进入 `JoinHashTable::ProbeSpill`，底层是 `RadixPartitionedColumnData`。

Build 侧 sink 路径：



```Plain Text
PhysicalHashJoin::Sink(right/build chunk)
    |
    +-- 执行 RHS join-key expressions
    +-- 构造 payload chunk
    +-- JoinHashTable::Build(append_state, keys, payload)
          |
          +-- 构造 source row:
          |       [condition keys, payload, optional found flag, hash]
          |
          +-- 根据 join 类型过滤 build-side NULL key
          +-- hash equality keys
          +-- sink_collection->AppendUnified(...)
                |
                v
            RadixPartitionedTupleData
                |
                +-- 每个 partition 是 TupleDataCollection
                +-- 里面的 blocks 可被 BufferManager evict 到 temp 文件
```



Hash Join 和 Sort 最大的区别是：Hash Join 的 build 侧 **从一开始就写入 radix partitioned collection**。初始 radix bits 在 \`HashJoinGlobalSinkState\` 里设置：



```Plain Text
initial_radix_bits = 4   // 线程数 < 100
initial_radix_bits = 5   // 线程数 >= 100
```



所以 build 数据的第一层分区不是等到 OOM 才发生，而是 sink 阶段就发生：



```Plain Text
JoinHashTable::Build(keys, payload):
    source_chunk = [keys, payload, optional_found_flag, hash]
    hash = Hash(equality_keys)
    sink_collection.AppendUnified(source_chunk)

RadixPartitionedTupleData::AppendUnified:
    partition_idx = radix(hash, radix_bits)
    append row to partitions[partition_idx]
```



伪代码：



```Plain Text
PhysicalHashJoin::Sink(build_chunk):
    join_keys = execute_rhs_key_exprs(build_chunk)
    payload = reference_payload_columns(build_chunk)
    local_ht.Build(local_append_state, join_keys, payload)

JoinHashTable::Build(append_state, keys, payload):
    if keys.empty:
        return

    source = DataChunk(layout_types)
    source[0..key_count) = keys
    source[key_count..key_count+payload_count) = payload
    if join_type needs found flag:
        source[found_col] = false

    hash_values = Hash(keys)
    source[hash_col] = hash_values

    added_sel = filter_null_build_keys_if_needed(keys)
    if added_sel.count == 0:
        return

    sink_collection.AppendUnified(append_state, source, added_sel)
```



这里的 `sink_collection` 是 `RadixPartitionedTupleData`，每个 partition 是 `TupleDataCollection`，内部 block 仍由 BufferManager 管理。



Finalize 时，Hash Join 会结合 `TemporaryMemoryState` 判断是否 external：



```Plain Text
PhysicalHashJoin::Finalize
    |
    +-- temporary_memory_state->UpdateReservation()
    +-- external = reservation < total_size
    |
    +-- 如果 in-memory:
    |       merge local HTs
    |       ht.Unpartition()
    |       schedule pointer-table finalize
    |
    +-- 如果 external:
            降低 load factor
            必要时增加 radix bits
            必要时 repartition local build data
            选择当前能放进内存的 build partitions
            对当前 partitions schedule pointer-table finalize
```



Finalize 的核心判断伪代码：



```Plain Text
PhysicalHashJoin::Finalize():
    temp_state.UpdateReservation(context)

    external = temp_state.reservation < total_size

    if external:
        ht.load_factor = EXTERNAL_LOAD_FACTOR

        temp_total_size, temp_max_partition_size, temp_max_partition_count =
            ht.GetTotalSize(local_hash_tables, load_factor = EXTERNAL_LOAD_FACTOR)

        if temp_total_size < temp_state.reservation:
            // 降低 load factor 后反而可以整体放下
            temp_state.minimum_reservation = temp_total_size
            temp_state.remaining_size = temp_total_size
            total_size = temp_total_size
            external = false

    if !external:
        for local_ht in local_hash_tables:
            global_ht.Merge(local_ht)
        global_ht.Unpartition()
        schedule HashJoinFinalizeEvent  // 构建完整 pointer table
        return READY

    // external hash join
    max_partition_ht_size =
        max_partition_size + ht.PointerTableSize(max_partition_count)

    very_skewed = max_partition_ht_size >= 0.8 * total_size

    if !very_skewed and
       max_partition_ht_size + probe_side_requirement > temp_state.reservation:
        ht.SetRepartitionRadixBits(
            max_ht_size = temp_state.reservation,
            max_partition_size,
            max_partition_count)
        schedule HashJoinRepartitionEvent
        return READY

    for local_ht in local_hash_tables:
        global_ht.Merge(local_ht)

    max_build_budget = temp_state.reservation - probe_side_requirement
    global_ht.PrepareExternalFinalize(max_build_budget)
    schedule HashJoinFinalizeEvent  // 只 finalize current partitions
```



这里可以明确回答“什么时候分区”：



```Plain Text
第一次分区:
    build sink 阶段就按 initial_radix_bits 分区。

二次 repartition:
    finalize 阶段发现 external join 必须启用，
    并且最大 build partition + pointer table + probe_side_requirement
    仍然超过 reservation，
    且数据不是极端 skew，
    才增加 radix bits 并触发 HashJoinRepartitionEvent。
```



`SetRepartitionRadixBits` 的目标不是刚好塞满内存，而是希望新的最大分区 hash table 估计值约小到 `max_ht_size / 4`：



```Plain Text
for added_bits in 1..:
    partition_multiplier = 2 ^ added_bits
    estimated_partition_size = max_partition_size / partition_multiplier
    estimated_partition_count = max_partition_count / partition_multiplier
    estimated_ht_size =
        estimated_partition_size + PointerTableSize(estimated_partition_count)

    if estimated_ht_size <= max_ht_size / 4:
        break

radix_bits += added_bits
```



`HashJoinRepartitionEvent` 会控制并发 repartition 的线程数，因为 repartition 本身也要占内存：



```Plain Text
thread_memory ~= 2 * blocks_per_vector * partition_multiplier * block_size
repartition_threads = max(reservation / thread_memory, 1)

如果 local_ht 数量 > repartition_threads:
    先把多余 local_ht merge 到保留的 local_ht 上

每个剩余 local_ht:
    local_ht.Repartition(global_ht)
```



External hash join 的核心是分批处理 build partitions：



```Plain Text
所有 build partitions
    |
    v
选择一批 unfinished partitions，使其 tuple data + pointer table 能放进 reservation
    |
    v
把这些 partitions 移入 JoinHashTable::data_collection
    |
    v
为当前 partitions 构建 pointer table
    |
    v
probe 属于当前 partitions 的 probe rows
    |
    v
不属于当前 partitions 的 probe rows 写入 ProbeSpill
    |
    v
下一轮继续处理剩余 build partitions
```



`JoinHashTable::PrepareExternalFinalize(max_ht_size)` 会从未完成的 build partitions 中选择一批，将它们标记为 current，并把对应 tuple data move/combine 到 `data_collection`。



更完整的伪代码：



```Plain Text
JoinHashTable::PrepareExternalFinalize(max_ht_size):
    if finalized:
        Reset data_collection / pointer table / current_partitions

    if completed_partitions all valid:
        return false

    unfinished = []
    min_partition_size = INF

    for partition_idx in all_partitions:
        if completed_partitions[partition_idx]:
            continue

        size = partitions[partition_idx].SizeInBytes()
             + PointerTableSize(partitions[partition_idx].Count())

        unfinished.push(partition_idx)
        min_partition_size = min(min_partition_size, size)

    stable_sort unfinished by:
        floor(partition_size / min_partition_size)

    count = 0
    data_size = 0

    for partition_idx in unfinished:
        incl_count = count + partitions[partition_idx].Count()
        incl_data_size = data_size + partitions[partition_idx].SizeInBytes()
        incl_ht_size = incl_data_size + PointerTableSize(incl_count)

        if count > 0 and incl_ht_size > max_ht_size:
            break

        current_partitions[partition_idx] = true
        data_collection.Combine(partitions[partition_idx])
        completed_partitions[partition_idx] = true

        count = incl_count
        data_size = incl_data_size

    return true
```



这里有两个细节：



```Plain Text
1. 至少会选择一个 partition
   即使单个 partition 已经超过 max_ht_size，也会继续处理，
   否则 external join 会无法前进。

2. 排序时保留接近原始 partition 顺序
   注释里说这样能减少 I/O，因为 partition index 会影响 eviction queue index。
```



Probe 侧 external 路径：



```Plain Text
PhysicalHashJoin::Execute
    |
    +-- 计算 probe keys
    +-- JoinHashTable::ProbeAndSpill(...)
          |
          +-- hash probe keys
          +-- RadixPartitioning::Select(...)
          |      |
          |      +-- true_sel: 属于当前已构建 partitions
          |      +-- false_sel: 属于未来 partitions
          |
          +-- false_sel rows:
          |      ProbeSpill::Append(spill_chunk)
          |         |
          |         v
          |      RadixPartitionedColumnData
          |
          +-- true_sel rows:
                 正常 probe 当前 pointer table
```



对应伪代码：



```Plain Text
PhysicalHashJoin::Execute(probe_chunk):
    if first time and sink.external:
        sink.InitializeProbeSpill()
        local.spill_state = sink.probe_spill.RegisterThread()

    probe_keys = execute_lhs_key_exprs(probe_chunk)

    if sink.external:
        hash_table.ProbeAndSpill(
            probe_keys,
            probe_chunk,
            sink.probe_spill,
            local.spill_state)
    else:
        hash_table.Probe(probe_keys)

    emit matches from scan_structure.Next(...)

JoinHashTable::ProbeAndSpill(probe_keys, probe_chunk):
    hashes = Hash(probe_keys)

    true_sel, false_sel =
        RadixPartitioning.Select(
            hashes,
            radix_bits,
            current_partitions)

    // 不属于当前 build partitions 的 probe rows，暂时不能 probe
    spill_chunk = probe_chunk + hashes
    spill_chunk = spill_chunk.Slice(false_sel)
    probe_spill.Append(spill_chunk)

    // 属于当前 build partitions 的 probe rows，立即 probe
    probe_keys = probe_keys.Slice(true_sel)
    probe_chunk = probe_chunk.Slice(true_sel)
    hashes = hashes.Slice(true_sel)

    InitializeScanStructure(...)
    GetRowPointers(..., hashes, current pointer table)
```



这里的“spill”也有两层：



```Plain Text
ProbeSpill::Append:
    算子语义上的 spill，意思是把未来 partition 的 probe rows 暂存起来。
    数据写进 RadixPartitionedColumnData。

BufferManager::WriteTemporaryBuffer:
    物理磁盘 spill。
    只有 ProbeSpill/ColumnDataCollection 背后的 blocks 被 unpin 后，
    又被 BufferPool evict，才真正写 temp 文件。
```



Probe spill 生命周期：



```Plain Text
ProbeSpill
  |
  +-- RegisterThread()
  |     创建 local RadixPartitionedColumnData + append state
  |
  +-- Append()
  |     按 hash radix 写入暂时不能 probe 的 probe rows
  |
  +-- Finalize()
  |     flush local states，并 combine 到 global partitions
  |
  +-- PrepareNextProbe()
        把当前 build partitions 对应的 probe partitions
        move 到 ColumnDataCollection
        创建 ColumnDataConsumer 扫描
```



每一轮 external join source 阶段的状态机：



```Plain Text
HashJoinGlobalSourceState::Initialize:
    if probe_spill exists:
        probe_spill.Finalize()
    global_stage = PROBE
    TryPrepareNextStage()

TryPrepareNextStage:
    BUILD 完成:
        hash_table.finalized = true
        PrepareProbe()

    PROBE 完成:
        if join type needs build-side scan:
            PrepareScanHT()
        else:
            PrepareBuild()

    SCAN_HT 完成:
        PrepareBuild()

PrepareBuild:
    remaining = ht.GetRemainingSize() + probe_side_requirement
    temp_state.SetRemainingSizeAndUpdateReservation(remaining)

    max_build_budget = temp_state.reservation - probe_side_requirement

    if !ht.PrepareExternalFinalize(max_build_budget):
        global_stage = DONE
        temp_state.SetZero()
        return

    allocate pointer table for current partitions
    global_stage = BUILD

PrepareProbe:
    if probe_spill:
        probe_spill.PrepareNextProbe()
    global_stage = PROBE
```



结论：Hash Join 的 external 不是简单地“把整个 hash table 写出去”。它用 radix partition 限制每轮内存工作集。build 侧和 probe spill 侧的数据仍然通过 DuckDB 的 collection 体系存储，最终由 BufferManager 负责 block eviction 和临时文件读写。



完整串起来可以看成：



```Plain Text
build sink:
    build rows -> radix partitions -> BufferManager blocks

finalize:
    if total build side fits:
        unpartition -> build full pointer table
    else:
        maybe repartition build side
        choose current build partitions
        build pointer table only for current partitions

probe:
    if probe row belongs to current partitions:
        probe now
    else:
        append to ProbeSpill partitions

source loop:
    after current probe done:
        choose next build partitions
        scan corresponding probe spill partitions
        repeat

physical disk spill:
    happens below these structures,
    when BufferManager evicts unpinned blocks.
```



## **Hash Aggregate 如何结合 BufferManager**



Hash Aggregate 的外层封装是 `RadixPartitionedHashTable`。sink 阶段每个线程有本地 `GroupedAggregateHashTable`，本地 hash table 中的 group row 和 aggregate state 存在 `RadixPartitionedTupleData` 中。



Sink 路径：



```Plain Text
PhysicalHashAggregate::Sink(input chunk)
    |
    +-- 从 aggregate child refs 构造 aggregate_input_chunk
    +-- 对每个 grouping set:
          |
          v
      RadixPartitionedHashTable::Sink(...)
          |
          +-- 如果 local GroupedAggregateHashTable 不存在则创建
          +-- PopulateGroupChunk
          +-- ht.AddChunk(group_chunk, aggregate_input_chunk)
                |
                +-- 在 pointer table 中 find/create group
                +-- 新 group append 到 partitioned tuple data
                +-- 初始化 aggregate states
                +-- 更新 aggregate states
```



`GroupedAggregateHashTable` 的内部结构：



```Plain Text
GroupedAggregateHashTable
  |
  +-- pointer table: ht_entry_t[]
  +-- partitioned_data: RadixPartitionedTupleData
  +-- optional unpartitioned_data
  +-- aggregate_allocator: aggregate 内部状态分配器
```



`MaybeRepartition` 是 Hash Agg sink 阶段的外部化判断点：



```Plain Text
local HT total_size =
    aggregate_allocator_size
  + partitioned_data.SizeInBytes()
  + pointer_table_bytes

if total_size > per-thread reservation:
    |
    +-- 尝试提高 TemporaryMemoryState reservation
    |
    +-- 如果仍然太大:
            设置 external radix bits
            从 local HT acquire partitioned data
            repartition 到 abandoned_data
            unpin 数据，使 BufferManager 可以 evict/spill
```



Sink 阶段还有自适应策略：



```Plain Text
多线程场景:
    初始 local HT capacity 较小
    用 HLL 估计唯一值数量
    如果几乎全是 unique:
        skip lookups，直接 append，后续统一 deduplicate
    如果扩大 HT 能明显提高去重率:
        增大 local capacity
    当 capacity 或 partition 压力变大:
        abandon local pointer table
        保留 tuple data
        必要时增加 radix bits

少线程场景:
    更偏 grow strategy
    仍然可能 external
    但 repartition 更保守
```



Combine 路径：



```Plain Text
RadixPartitionedHashTable::Combine
    |
    +-- MaybeRepartition(..., combine = true)
    +-- ht.AcquirePartitionedData()
          |
          +-- flush append state
          +-- unpin partitioned data
          +-- return PartitionedTupleData
    |
    +-- merge 到 global uncombined_data
    +-- 保存 aggregate allocators，保证 aggregate state 引用仍有效
```



Finalize 和 Source 路径：



```Plain Text
RadixPartitionedHashTable::Finalize
    |
    +-- 把 global uncombined_data 拆成 AggregatePartition
    +-- 计算 max_partition_size
    +-- minimum reservation =
          stored aggregate allocator bytes + max_partition_size
    +-- source 真正开始前 remaining_size 设为 0

HashAggregate source phase
    |
    +-- MaxThreads 请求:
          stored_allocators_size + max_threads * max_partition_size
    |
    +-- 每个 source worker 获取一个 partition
          |
          +-- 如果 partition 尚未 finalize:
          |      创建临时 GroupedAggregateHashTable
          |      combine partition rows，完成去重/聚合合并
          |      用合并后的 data 替换 partition data
          |
          +-- scan partition data
          +-- RowOperations::FinalizeStates
          +-- 根据 pin policy 销毁或释放 blocks
```



Hash Agg 接入 BufferManager 有两层含义：



1. group rows 和 aggregate states 存在 `TupleDataCollection` blocks 中，这些 blocks 可以 spill。

2. `TemporaryMemoryManager` 控制 sink 阶段什么时候 external/repartition，也控制 source 阶段可以并发 finalize 多少 partitions。

需要注意：aggregate state 里通过 `ArenaAllocator` 分配的内存也会走 BufferManager allocator 的统计路径，但它不像 tuple block 那样可以独立按 partition spill。因此 source 阶段计算并发度时，会先扣掉 `stored_allocators_size`，再看剩余 reservation 能放下多少个 partition。



## **Sort、Hash Join、Hash Agg 对比**



```Plain Text
+-------------------------+----------------------+-------------------------+
                | 外部化工作单元          | spill 数据形态       | 内存决策者              |
+---------------+-------------------------+----------------------+-------------------------+
| Sort          | SortedRun               | TupleDataCollection  | SortGlobalSinkState +   |
|               |                         | key/payload blocks   | TemporaryMemoryState    |
+---------------+-------------------------+----------------------+-------------------------+
| Hash Join     | radix build partition   | RadixPartitioned     | HashJoinGlobalSinkState |
|               | plus probe spill        | TupleData/ColumnData | + TemporaryMemoryState  |
+---------------+-------------------------+----------------------+-------------------------+
| Hash Aggregate| aggregate partition     | RadixPartitioned     | RadixHTGlobalSinkState  |
|               |                         | TupleData blocks     | + TemporaryMemoryState  |
+---------------+-------------------------+----------------------+-------------------------+
```



共同模式：



```Plain Text
operator-specific data structure
    |
    v
TupleDataCollection / ColumnDataCollection
    |
    v
BlockHandle / BufferHandle
    |
    v
StandardBufferManager
    |
    +-- 内存足够:
    |       block 保持 loaded
    |
    +-- 内存紧张:
            BufferPool eviction queue
            -> WriteTemporaryBuffer
            -> .tmp 或 .block 文件
            -> 后续 Pin 时 reload
```



最终结论：



DuckDB 的 Sort、Hash Join、Hash Aggregate 并不是每个算子各自实现一套独立 spill 文件格式。它们主要负责决定：



- 什么时候 external；

- 以 run 还是 partition 为单位切分工作集；

- 每轮处理哪些数据；

- 什么时候 unpin 或 destroy 已处理 block。

底层 block 生命周期统一交给 BufferManager：



```Plain Text
allocate -> pin -> use -> unpin -> eviction queue -> spill file -> reload
```



这种设计让外部算子的复杂度集中在“如何切分和调度工作集”，而不是重复实现文件级 spill 管理。


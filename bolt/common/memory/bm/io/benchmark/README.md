# DiskIoScheduler 与 fio io_uring 对比 Benchmark 方案

## 目标

在 `bolt/common/memory/bm/io/benchmark` 下设计一组 benchmark，用来对比：

1. fio 直接使用 `io_uring` 的性能。
2. Bolt `DiskIoScheduler` 走生产调度路径的性能。

这个 benchmark 重点回答两个问题：

1. 在相同文件、相同 IO 模式、相同 block size、相同 queue depth 下，
   `DiskIoScheduler` 相比 fio `io_uring` 有多少端到端开销。
2. 如果有明显差距，差距更可能来自哪里：请求排队、优先级调度、自适应
   depth、completion 处理、future 唤醒，还是 buffer 准备。

这里比较的是 **端到端 scheduler overhead**，不是裸 `io_uring` syscall
overhead。fio 是成熟的 userspace IO 压测工具；`DiskIoScheduler` 包含 BM
自己的队列、调度、统计和 future 语义，两者内部不会完全等价。

## 推荐实现

推荐拆成两个部分：

```text
run_scheduler_vs_fio.sh
bolt_memory_bm_io_scheduler_benchmark
```

职责划分：

1. `run_scheduler_vs_fio.sh` 是对比入口，负责生成 fio job、调用 fio、调用
   scheduler benchmark binary、解析两边输出并打印对比表。
2. `bolt_memory_bm_io_scheduler_benchmark` 只负责跑 `DiskIoScheduler` 路径，
   不调用 fio，也不解析 fio JSON。

这样做的原因：

1. fio 保持外部基线工具形态，脚本里能直接看到 fio job 参数。
2. C++ benchmark 不依赖 fio，不需要把外部命令执行和 JSON 解析塞进 binary。
3. 对比逻辑集中在脚本里，后续增加机器信息采集、批量矩阵运行、结果归档更方便。

## fio 调用方式

fio 从脚本里调用，不从 C++ benchmark 里调用。

脚本路径建议：

```bash
bolt/common/memory/bm/io/benchmark/run_scheduler_vs_fio.sh
```

脚本默认跑一个带宽场景和一个 IOPS 场景：

```bash
bolt/common/memory/bm/io/benchmark/run_scheduler_vs_fio.sh
```

也可以通过环境变量指定场景：

```bash
SCENARIO=iops_read BS=4k IODEPTH=256 NUMJOBS=4 \
  bolt/common/memory/bm/io/benchmark/run_scheduler_vs_fio.sh
```

常用参数：

```text
FIO_BIN=fio
SCHEDULER_BENCHMARK=_build/Release/bolt/common/memory/bm/io/benchmark/bolt_memory_bm_io_scheduler_benchmark
FILE=/tmp/bolt-bm-io-benchmark.dat
SIZE=4g
SCENARIO=bandwidth_read,bandwidth_write,iops_read,iops_write,all
BS=256k              # 未指定时由 SCENARIO 决定默认值
IODEPTH=128          # 未指定时由 SCENARIO 决定默认值
NUMJOBS=1
RUNTIME=30
DIRECT=0
FIO_INVALIDATE=0
FIO_FORCE_ASYNC=0      # 诊断项，非默认对比路径
FIO_NORANDOMMAP=1
DROP_CACHES=0         # 设置为 1 时，每次跑 fio/scheduler 前清 OS page cache
DROP_CACHES_DEBUG=0   # 设置为 1 时，打印 drop 前后的 /proc/meminfo
ORDER=scheduler_first
OUTPUT_DIR=/tmp/bolt-bm-io
```

脚本应输出：

1. fio job 文件。
2. fio JSON 结果。
3. scheduler 结果。
4. 一张对比表，包含 IOPS、带宽、p50/p99 latency 和错误数。

`DIRECT` 默认是 `0`。原因是当前 BM `IoBuffer::allocateFromMalloc()` 不保证
满足 `O_DIRECT` 对齐要求。等 scheduler 路径支持对齐 buffer 后，再加
`DIRECT=1` 作为单独 case。

## 测试场景分类

场景分成 IOPS 测试和带宽测试，不混在一张矩阵里解释。两类场景的主要指标、
block size、queue depth 都不同。

### IOPS 测试场景

IOPS 场景使用小 block、高 queue depth，重点观察单次 IO 调度开销、completion
处理和 future 唤醒对吞吐的影响。

主要指标：

1. IOPS
2. p50 / p99 latency
3. scheduler 相对 fio 的 IOPS 差距

| 场景 | fio `rw` | Scheduler op | Block size | Queue depth | Jobs |
| --- | --- | --- | --- | --- | --- |
| `iops_read` | `randread` | Read | 4 KiB, 16 KiB | 128, 256 | 1, 4 |
| `iops_write` | `randwrite` | Write | 4 KiB, 16 KiB | 128, 256 | 1, 4 |

### 带宽测试场景

带宽场景使用大 block、顺序 IO，重点观察 BM spill read/write 类 workload 下
大块 IO 的吞吐上限。

主要指标：

1. MiB/s
2. p50 / p99 latency
3. scheduler 相对 fio 的带宽差距

| 场景 | fio `rw` | Scheduler op | Block size | Queue depth | Jobs |
| --- | --- | --- | --- | --- | --- |
| `bandwidth_read` | `read` | Read | 256 KiB, 1 MiB, 4 MiB | 32, 128 | 1 |
| `bandwidth_write` | `write` | Write | 256 KiB, 1 MiB, 4 MiB | 32, 128 | 1 |

### 场景默认值

脚本可以按 `SCENARIO` 设置默认参数：

| SCENARIO | fio `rw` | 默认 BS | 默认 IODEPTH | 默认 NUMJOBS | 主要指标 |
| --- | --- | --- | --- | --- | --- |
| `bandwidth_read` | `read` | 1 MiB | 128 | 1 | bandwidth |
| `bandwidth_write` | `write` | 1 MiB | 128 | 1 | bandwidth |
| `iops_read` | `randread` | 4 KiB | 256 | 1 | IOPS |
| `iops_write` | `randwrite` | 4 KiB | 256 | 1 | IOPS |

环境变量显式指定 `BS`、`IODEPTH`、`NUMJOBS` 时覆盖这些默认值。
默认执行顺序是 `ORDER=scheduler_first`，也可以显式指定
`ORDER=fio_first` 做对照。fio 和 scheduler 会使用由 `FILE` 派生出的不同
数据文件，避免同一个 buffered 文件先后访问导致 page cache 状态直接继承。

脚本默认设置 `FIO_INVALIDATE=0`。fio 的默认值是 `invalidate=1`，会在 job
开始前主动 invalidate page cache；`DiskIoScheduler` 路径没有这个动作。
buffered 场景下如果 fio 保持默认值，就会把 fio 放在冷 cache 条件下测试，
而 scheduler 走正常 buffered 路径，结果会被 fio 的 cache 处理方式放大。

`FIO_FORCE_ASYNC=1` 可以作为诊断开关传给 fio，用来观察 `IOSQE_ASYNC` 对
buffered io_uring 的影响；默认不启用，因为当前 `DiskIoScheduler` backend
没有设置 `IOSQE_ASYNC`，默认启用会改变对比对象。

脚本默认设置 `FIO_NORANDOMMAP=1`。fio 的 random map 会尽量保证一轮内不重复
访问同一个 block；scheduler 当前实现会循环使用随机 offset 序列，运行足够久时
会重复访问同一文件的数据。buffered random read 场景下，为了让 fio 的 cache
命中模型更接近 scheduler，默认关闭 fio random map。

如果需要比较冷 page cache 条件，可以设置 `DROP_CACHES=1`。脚本会在每次运行
fio 或 scheduler 前执行 `sync` 和 `sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'`。
这个操作会影响整机 page cache，只建议在专用测试机器上使用。
如果需要确认命令是否影响 page cache，可以同时设置 `DROP_CACHES_DEBUG=1`，
脚本会打印 drop 前后的 `Cached`、`Buffers`、`Dirty`、`Writeback`。

第一版优先实现：

1. `bandwidth_read`
2. `bandwidth_write`
3. `iops_read`
4. `iops_write`
5. 单 job
6. `direct=0`
7. `invalidate=0`
8. `norandommap=1`

多 job 并发和完整矩阵可以第二阶段再补。

## C++ benchmark 参数

C++ benchmark 只跑 scheduler 路径，建议暴露这些 flag：

```text
--bm_io_benchmark_path=/tmp/bolt-bm-io-benchmark.dat
--bm_io_benchmark_file_size_mb=4096
--bm_io_benchmark_block_size_kb=256
--bm_io_benchmark_queue_depth=128
--bm_io_benchmark_jobs=1
--bm_io_benchmark_runtime_sec=30
--bm_io_benchmark_scenario=bandwidth_read|bandwidth_write|iops_read|iops_write
--bm_io_benchmark_output_json=/tmp/bolt-bm-io/scheduler.json
```

随机 IO 需要固定 seed。fio 侧使用 `randrepeat=1`，scheduler 侧用相同 seed
生成 offset 序列，保证多次运行可复现。

## 公平性约束

1. fio 和 scheduler 使用同一组场景参数、文件大小，但使用不同文件路径。
2. 读测试前预创建并预分配文件，setup 不计入耗时。
3. 两边都做 warmup，warmup 不计入耗时。
4. 两边使用相同 block size、queue depth、运行时长和随机 offset 规则。
5. CPU affinity 只作为可选参数，不写死到 benchmark。
6. 输出里明确标记 `direct=0` 还是 `direct=1`。
7. scheduler 写路径每个请求都会转移 `IoBuffer` 所有权；为了不把重复 malloc
   误算成 scheduler 开销，C++ benchmark 在计时前预分配 buffer pool。

## fio job 模板

脚本应生成 fio job：

```ini
[global]
ioengine=io_uring
filename=${path}
size=${file_size}
bs=${block_size}
iodepth=${queue_depth}
numjobs=${jobs}
runtime=${runtime_sec}
time_based=1
group_reporting=1
randrepeat=1
norandommap=1
invalidate=0
direct=0

[workload]
rw=${rw}
```

执行方式：

```text
fio --output-format=json --output=<json_path> <job_path>
```

JSON 至少解析这些字段：

1. `read.iops` / `write.iops`
2. `read.bw_bytes` / `write.bw_bytes`
3. `read.clat_ns.percentile` / `write.clat_ns.percentile`
4. error count

如果机器上没有 fio，脚本应直接失败并提示安装 fio；C++ benchmark 不处理 fio
缺失问题。

## Scheduler 路径设计

Scheduler 路径做这些事：

1. 准备同一批 offset 和 buffer。
2. 最多保持 `queue_depth * jobs` 个请求 inflight。
3. 通过 `diskIoScheduler().submit(std::move(request))` 提交请求。
4. 每完成一个 future，就补一个新请求，直到运行时间结束。
5. 统计完成 IO 数、完成字节数、submit latency、端到端 latency、错误数。
6. 到达 deadline 后停止提交新请求，然后 drain 已经提交的 futures。

写请求需要 fresh `IoBuffer`，因为 `IoRequest` 会接管 buffer。benchmark 在计时前
按 inflight 上限预分配 buffer pool；submit 时从 free list 取 buffer，future
完成后从 `IoResult` 回收到 free list。write buffer 只在预分配阶段填充一次，
避免把每请求 malloc/free 和重复 fill 计入 scheduler 性能。

## 输出格式

脚本输出一张紧凑表：

```text
scenario         backend      bs    qd   jobs   iops       bw_MiBps   p50_us  p99_us  errors
bandwidth_read   fio          1M    128  1      ...
bandwidth_read   scheduler    1M    128  1      ...
bandwidth_read   overhead     1M    128  1      iops=-x%   bw=-y%     p99=+z%
iops_read        fio          4K    256  1      ...
iops_read        scheduler    4K    256  1      ...
iops_read        overhead     4K    256  1      iops=-x%   bw=-y%     p99=+z%
```

结尾打印 `diskIoScheduler().stats()`，便于把慢 case 关联到 queue depth、
inflight、dispatch 和 completion 统计。

## 文件规划

```text
bolt/common/memory/bm/io/benchmark/
  CMakeLists.txt
  SchedulerBenchmark.cpp
  run_scheduler_vs_fio.sh
  README.md
```

`CMakeLists.txt` 建议：

```cmake
add_executable(
  bolt_memory_bm_io_scheduler_benchmark
  SchedulerBenchmark.cpp)

target_link_libraries(
  bolt_memory_bm_io_scheduler_benchmark
  PRIVATE bolt_memory_bm_io
          bolt_common_base
          ${FOLLY_BENCHMARK}
          glog::glog)
```

`bolt/common/memory/bm/io/CMakeLists.txt` 增加：

```cmake
if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmark)
endif()
```

## 风险

1. fio 和 `DiskIoScheduler` 内部语义不同，结果只能解释为端到端 scheduler
   overhead。
2. `direct=1` 需要 scheduler buffer 满足对齐要求，否则结果不可靠，甚至直接失败。
3. 小文件会被 page cache 强烈影响。正式报告结果时要记录文件大小、机器内存和
   `direct` 设置。
4. fio JSON 解析可以先做最小实现，但字段缺失时必须明确失败，不能静默输出 0。

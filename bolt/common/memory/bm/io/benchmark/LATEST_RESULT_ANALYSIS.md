# 最新运行结果分析

本文整理 `/data00/home/wangxinshuo.db/bolt/log.txt` 中的最新运行结果。

## 运行条件

本轮运行使用 buffered IO：

```text
direct=0
invalidate=0
norandommap=1
DROP_CACHES=1
DROP_CACHES_DEBUG=1
runtime=90s
fio 和 scheduler 使用不同数据文件
```

`drop_caches` 在每次运行 fio 或 scheduler 前执行：

```bash
sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

## Page Cache 清理结果

日志显示 `drop_caches` 生效。典型样例：

```text
drop_caches before:
Buffers:          242024 kB
Cached:         18046936 kB
Dirty:           2982216 kB
Writeback:        122244 kB

drop_caches after:
Buffers:            6060 kB
Cached:          2605476 kB
Dirty:               608 kB
Writeback:             0 kB
```

write 场景前也能看到 dirty page 被清理：

```text
drop_caches before:
Cached:         13242368 kB
Dirty:          10029068 kB
Writeback:        237676 kB

drop_caches after:
Cached:          2604736 kB
Dirty:                84 kB
Writeback:             0 kB
```

结论：OS page cache 和 dirty/writeback 状态确实在每次 benchmark 前被重置到较低水平。

## 性能结果汇总

| 场景 | 后端 | Block | QD | IOPS | BW MiB/s | p50 us | p99 us | errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| bandwidth_read | fio | 1 MiB | 128 | 2379.02 | 2379.02 | 20316.16 | 717225.98 | 0 |
| bandwidth_read | scheduler | 1 MiB | 128 | 5575.83 | 5575.83 | 24703.00 | 54286.00 | 0 |
| bandwidth_write | fio | 1 MiB | 128 | 2528.52 | 2528.52 | 33816.58 | 708837.38 | 0 |
| bandwidth_write | scheduler | 1 MiB | 128 | 2936.19 | 2936.19 | 34396.00 | 387351.00 | 0 |
| iops_read | fio | 4 KiB | 256 | 12384.48 | 48.38 | 25559.04 | 26607.62 | 0 |
| iops_read | scheduler | 4 KiB | 256 | 490409.00 | 1915.66 | 519.00 | 840.00 | 0 |
| iops_write | fio | 4 KiB | 256 | 377502.38 | 1474.62 | 667.65 | 864.26 | 0 |
| iops_write | scheduler | 4 KiB | 256 | 478280.00 | 1868.28 | 448.00 | 1066.00 | 0 |

相对 fio 的 scheduler 结果：

```text
bandwidth_read   +134.38%
bandwidth_write  +16.12%
iops_read        +3859.87%
iops_write       +26.70%
```

## Scheduler 细分耗时

### bandwidth_write

`bandwidth_write` 是本轮重点关注场景。

```text
average_device_latency_us=43574
max_latency_us=1733065
average_queue_wait_us=5.78598
max_queue_wait_us=266
average_backend_submit_us=0.0158232
max_backend_submit_us=225
average_backend_reap_us=0.0022088
max_backend_reap_us=224
average_worker_wait_us=166.412
max_worker_wait_us=1690606
average_future_fulfill_us=1.1083
max_future_fulfill_us=257
average_submit_batch_size=1.00045
average_completion_batch_size=1.00001
max_observed_inflight_requests=128
```

这些数据说明：

1. `max_observed_inflight_requests=128`，scheduler 能把 fixed depth 打满。
2. `average_queue_wait_us=5.78`，请求没有堵在 scheduler pending queue。
3. `average_backend_submit_us=0.0158`，`io_uring_submit` 路径不是瓶颈。
4. `average_backend_reap_us=0.0022`，reap CQE 不是瓶颈。
5. `average_future_fulfill_us=1.1083`，future fulfill 不是瓶颈。
6. `average_device_latency_us=43574`，长耗时发生在 submit 后等待内核 completion 的阶段。
7. `max_worker_wait_us=1690606` 和 `max_latency_us=1733065` 接近，说明长尾期间 worker 主要在 epoll 等待 completion event。

因此，`bandwidth_write` 的瓶颈不在 scheduler 用户态 submit/reap/future 路径，而在 buffered write 的内核 completion/writeback 路径。

### bandwidth_read

```text
average_device_latency_us=4590.47
average_queue_wait_us=9498.81
average_backend_submit_us=177.849
average_backend_reap_us=1.60708
average_future_fulfill_us=21.8123
average_submit_batch_size=49.6144
average_completion_batch_size=49.6144
max_observed_inflight_requests=120
```

read 场景 completion batch 较大，submit/completion batch size 都约为 49。这里 `average_backend_submit_us=177.849` 明显高于 write，说明 buffered read 场景可能有一部分工作在 `io_uring_submit()` 调用内完成或阻塞，但整体吞吐仍高于 fio。

### iops_write

```text
average_device_latency_us=235.113
max_latency_us=965614
average_queue_wait_us=191.435
average_backend_submit_us=0.0347318
average_backend_reap_us=0.602772
average_future_fulfill_us=1.6927
average_submit_batch_size=7.15592
average_completion_batch_size=7.04089
```

4 KiB write 下 scheduler 的平均 submit/reap/future 开销很低，吞吐高于 fio。`max_latency_us` 和 `max_worker_wait_us` 仍出现长尾，说明小块 write 也可能遇到内核侧偶发等待，但平均影响较小。

### iops_read

```text
average_device_latency_us=110.557
max_latency_us=595
average_queue_wait_us=193.131
average_backend_submit_us=1.02696
average_backend_reap_us=1.71235
average_future_fulfill_us=26.7312
average_submit_batch_size=94.5906
average_completion_batch_size=94.5906
```

4 KiB read 的 scheduler 路径非常快，completion batch 较大。需要注意的是，在 `DROP_CACHES=1` 且 buffered read 场景下，scheduler 因为运行速度更快，会更快把自己的文件读热并反复命中 page cache；因此 `iops_read` 的巨大领先不能直接解释为纯 scheduler 开销优势。

## 结论

1. `drop_caches` 已确认生效，日志中 `Cached`、`Dirty`、`Writeback` 在 drop 后明显下降。
2. 本轮四个场景均 `errors=0`。
3. `bandwidth_write` 中 scheduler 相比 fio 快 16.12%，但两者都有明显 p99 长尾。
4. 细分耗时显示，`bandwidth_write` 的长尾不来自 scheduler pending queue、backend submit、backend reap 或 future fulfill。
5. `bandwidth_write` 的长尾主要发生在 submit 后等待内核 completion 的阶段，结合 dirty page 和 writeback 变化，瓶颈应归因于 buffered write/writeback completion 延迟。
6. `average_submit_batch_size=1` 是完成一个补一个的稳态表现，不是吞吐低的直接根因。
7. `iops_read` 在冷 cache + buffered 场景下仍需谨慎解读，因为 scheduler 更快进入热 cache 循环，fio 冷读占比更高。

## 后续建议

1. 如果要进一步验证 writeback 影响，可以对比 `BS=256k`、`BS=512k`、`BS=1m` 的 `bandwidth_write`，观察 p99 和 `average_device_latency_us` 是否随 block size 增大。
2. 如果要比较纯 IO 栈，后续需要支持 aligned buffer 后增加 `DIRECT=1` 场景。
3. 如果要评估生产 buffered 行为，当前 `DROP_CACHES=1` 结果适合作为冷 cache 对照；建议同时保留 `DROP_CACHES=0` 的热 cache/稳态结果。

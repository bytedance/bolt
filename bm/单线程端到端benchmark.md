# 单线程端到端benchmark

# **2026\-06\-18 BM RowContainer pipeline benchmark 运行结果分析**



## **数据来源**



- 运行日志目录：`/data00/home/wangxinshuo.db/bolt/log/bolt-bm-row-container-pipeline-20260617-231002`

- benchmark stdout：`stdout.txt`

- runner stderr 与 pipeline metric：`stderr.txt`

- 本文只分析本次 pipeline 运行结果，不和历史运行结果对比。

- 命名说明：日志里的 `variable` 在本文统一记为 `variable_small`；`variable_large` 是并列数据集。

- 当时 runner regex 未加锚点，`*_variable` case 会同时输出 `*_variable_large`。本文对 `variable_large` 使用后续单独 case；`variable_small` 使用旧名 `variable` 对应结果。

## **运行概况**



- 一共运行 18 个 case，声明总数 18，全部 `exit=0`。

- 每个 case 单独进程运行，命令里统一使用：

    - `--bm_row_container_data_bytes=26843545600`，即 25\.00 GiB 逻辑数据量。

    - `--bm_row_container_warmup_data_bytes=134217728`，即 0\.12 GiB warmup 数据量。

    - `timeout 900s`。

- pipeline benchmark 覆盖 end\-to\-end 链路：store、spill write、spill read、read。BM 路径的 store 使用 batch append。

## **总体结论**



1. 本次 single\-thread pipeline 中，BM RowContainer 在全部 9 个 dataset/compression 组合上都快于 old RowContainer。

2. 最大总加速来自 `fixed/raw`，为 5\.62x；最小总加速来自 `variable_large/zstd`，为 1\.33x。

3. fixed raw 仍是最稳定的强收益场景，主要来自 store 和 spill read。

4. `variable_small` 与 `variable_large` 的压缩收益差异很大：`variable_small` 物理 spill 仍在十几到几十 GiB 级别，`variable_large` lz4/zstd 可压到约 1\.1 GiB/0\.8 GiB。

5. zstd 场景虽然物理写出更少，但压缩 CPU 抵消了相当一部分 BM 容器收益。

## **总耗时**



|dataset|compression|old total|BM total|BM 加速比|BM / old|
|---|---|---|---|---|---|
|fixed|raw|3\.66min|39\.04s|5\.62x|17\.8%|
|fixed|lz4|3\.80min|1\.52min|2\.49x|40\.1%|
|fixed|zstd|6\.10min|4\.08min|1\.50x|66\.8%|
|variable\_small|raw|2\.57min|50\.62s|3\.05x|32\.8%|
|variable\_small|lz4|3\.63min|1\.67min|2\.17x|46\.2%|
|variable\_small|zstd|5\.40min|3\.56min|1\.52x|66\.0%|
|variable\_large|raw|1\.80min|30\.57s|3\.52x|28\.4%|
|variable\_large|lz4|40\.07s|24\.31s|1\.65x|60\.7%|
|variable\_large|zstd|46\.01s|34\.58s|1\.33x|75\.1%|



## **fixed 场景**



### **阶段耗时**



|compression|impl|store|spill write|spill read|read|total|
|---|---|---|---|---|---|---|
|raw|old|25\.68s|42\.41s|2\.40min|7\.03s|3\.66min|
|raw|BM|6\.71s|8\.95s|18\.80s|4\.58s|39\.04s|
|lz4|old|25\.80s|1\.58min|1\.67min|7\.02s|3\.80min|
|lz4|BM|6\.73s|62\.02s|18\.03s|4\.59s|1\.52min|
|zstd|old|25\.79s|3\.64min|1\.91min|7\.05s|6\.10min|
|zstd|BM|6\.73s|2\.94min|56\.70s|4\.58s|4\.08min|



### **阶段加速比**



|compression|store|spill write|spill read|read|total|
|---|---|---|---|---|---|
|raw|3\.83x|4\.74x|7\.67x|1\.53x|5\.62x|
|lz4|3\.84x|1\.53x|5\.56x|1\.53x|2\.49x|
|zstd|3\.83x|1\.24x|2\.02x|1\.54x|1\.50x|



### **物理 spill 大小**



|compression|old spill bytes|BM physical spill bytes|BM / old|
|---|---|---|---|
|raw|26\.25 GiB|30\.00 GiB|114\.3%|
|lz4|25\.39 GiB|25\.86 GiB|101\.9%|
|zstd|22\.00 GiB|21\.69 GiB|98\.6%|



## **variable\_small 场景**



### **阶段耗时**



|compression|impl|store|spill write|spill read|read|total|
|---|---|---|---|---|---|---|
|raw|old|20\.56s|28\.93s|1\.57min|10\.51s|2\.57min|
|raw|BM|8\.72s|13\.16s|22\.45s|6\.29s|50\.62s|
|lz4|old|20\.80s|87\.42s|1\.65min|10\.46s|3\.63min|
|lz4|BM|8\.72s|63\.27s|22\.08s|6\.38s|1\.67min|
|zstd|old|20\.74s|2\.86min|2\.02min|10\.42s|5\.40min|
|zstd|BM|8\.73s|2\.62min|41\.51s|6\.31s|3\.56min|



### **阶段加速比**



|compression|store|spill write|spill read|read|total|
|---|---|---|---|---|---|
|raw|2\.36x|2\.20x|4\.20x|1\.67x|3\.05x|
|lz4|2\.39x|1\.38x|4\.48x|1\.64x|2\.17x|
|zstd|2\.37x|1\.09x|2\.92x|1\.65x|1\.52x|



### **物理 spill 大小**



|compression|old spill bytes|BM physical spill bytes|BM / old|
|---|---|---|---|
|raw|32\.21 GiB|37\.74 GiB|117\.2%|
|lz4|18\.99 GiB|19\.87 GiB|104\.6%|
|zstd|15\.27 GiB|15\.81 GiB|103\.5%|



## **variable\_large 场景**



### **阶段耗时**



|compression|impl|store|spill write|spill read|read|total|
|---|---|---|---|---|---|---|
|raw|old|5\.60s|14\.09s|85\.01s|3\.00s|1\.80min|
|raw|BM|5\.17s|9\.13s|13\.45s|2\.83s|30\.57s|
|lz4|old|5\.60s|13\.17s|18\.36s|2\.94s|40\.07s|
|lz4|BM|5\.15s|6\.33s|10\.05s|2\.78s|24\.31s|
|zstd|old|5\.60s|17\.21s|20\.24s|2\.97s|46\.01s|
|zstd|BM|4\.94s|14\.77s|12\.02s|2\.85s|34\.58s|



### **阶段加速比**



|compression|store|spill write|spill read|read|total|
|---|---|---|---|---|---|
|raw|1\.09x|1\.54x|6\.32x|1\.06x|3\.52x|
|lz4|1\.09x|2\.08x|1\.83x|1\.06x|1\.65x|
|zstd|1\.13x|1\.17x|1\.68x|1\.04x|1\.33x|



### **物理 spill 大小**



|compression|old spill bytes|BM physical spill bytes|BM / old|
|---|---|---|---|
|raw|25\.41 GiB|25\.87 GiB|101\.8%|
|lz4|1\.15 GiB|1\.12 GiB|97\.2%|
|zstd|0\.80 GiB|0\.78 GiB|98\.6%|



## **分析**



- fixed raw 的 BM 总耗时为 39\.04s，old 为 3\.66min，总加速 5\.62x。

- `variable_small` raw/lz4/zstd 总加速分别为 3\.05x, 2\.17x, 1\.52x；收益主要来自 spill read，store/read 也有明显改善。

- `variable_large` raw 由于 old spill read 很慢，本次总加速达到 3\.52x；lz4/zstd 则分别为 1\.65x 和 1\.33x。

- 对 `variable_large`，lz4/zstd 的物理写出明显低于 raw，但 zstd 的总耗时仍高于 lz4，说明压缩 CPU 仍是主导因素。

## **需要关注的点**



1. single\-thread pipeline 中 BM 全部领先，说明 RowContainer \-\> BM RowContainer 的端到端收益在 store/spill/read 串联后仍然成立。

2. `variable_small` 是新增重点：它不是 `variable_large` 的缩小版，而是行数更多、小字符串更多的场景。

3. fixed 和 `variable_small` 的 raw/lz4 场景对 Window 类 operator 更有参考价值，因为这些场景更能暴露容器批量写入与批量读取收益。

4. zstd 场景继续优化前，应优先拆分压缩 CPU 与容器元数据成本，否则容易把压缩瓶颈误判为容器瓶颈。


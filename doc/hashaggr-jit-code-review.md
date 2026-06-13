# Hash Aggregation JIT 代码 Review 清单

> 对比范围：`d4e69030bfbe1d27eb31e6ad49027833bfce2c8e..HEAD`（hash aggr jit 支持代码，~7956 行 / 40 文件）
> 用途：逐条优化的工作清单。**本文档仅记录问题与建议，不含代码修改。**
> 关注维度：① 代码坏味道 ② JIT 与非 JIT 关键数据结构（raw input / intermediate / group / result）一致性 ③ 框架与聚合函数耦合残留 ④ 数据结构冗余

---

## 0. 总体结论

- **架构方向正确**：`HashAggrJitOps` 回调结构体已把各聚合语义下沉到 `ops/`，框架三大骨架 `genInitIR/genAddDenseIR/genExtractIR` 没有 `switch(kind)` 大分支。
- **数据布局基准一致**：accumulator 起始 offset、null byte/mask 全部来自框架 `Aggregate::createHashAggrJitSlot`（`Aggregate.cpp:335`），JIT 不硬编码；内部字段用 `offsetof + static_assert(is_standard_layout)` 锁定。6 对聚合实现的字段顺序、ROW 子列顺序、null 标记、溢出语义**当前全部一致**。
- **主要待优化点**：
  1. decimal 的 IR 生成（add-with-overflow / extract）泄漏在框架 `HashAggrJitCodegen` 中（耦合）。
  2. 三大 gen 函数与 Sum/MinMax ops 存在大量样板重复（坏味道）。
  3. JIT decimal state 与非 JIT accumulator 是「镜像复制」而非源码共享，缺跨编译单元交叉断言（一致性风险 + 冗余）。
  4. descriptor 的 decimal 专属字段被所有 slot 冗余携带（数据冗余）。

优先级建议：先做 **C1（解耦 decimal）+ S1/S2（消骨架与 Sum/MinMax 重复）+ D1/D2（消 decimal 双定义与死字段）**，收益最大。

---

## 1. 框架与聚合函数耦合残留（关注点③）

| # | 位置 | 问题 | 建议 | 严重度 |
|---|---|---|---|---|
| **C1** | `HashAggrJit.cpp:797-841` `emitDecimalSumExtract`/`emitDecimalAvgExtract`；`843-874` `emitDecimalAddWithOverflow`；头文件 `HashAggrJit.h:266-291` | decimal 专属 IR 生成（i128 累加+overflow 进位、调 decimal runtime、读 precision/scale/auxPrecision/auxScale、long/short decimal 判断）是**框架类 `HashAggrJitCodegen` 的成员**；ops 只是转手调用。decimal 知识泄漏进框架。 | 下沉到 `DecimalSumOps.cpp`/`DecimalAvgOps.cpp` 内 `static` 辅助函数，只依赖通用原语（loadValue/storeValue/builder/module）。框架类不应有任何带 "Decimal" 的方法。 | 高 ✅ **已完成** |
| **C2** | `HashAggrJit.cpp:118-148` `ensureBuiltinDeclarations` | 框架构造函数无条件声明 4 个 decimal extract runtime 签名，即使 chunk 内无 decimal。 | 给 `HashAggrJitOps` 增加可选 `declareRuntime(llvm::Module&)` 回调，由各 decimal ops 自行声明；框架只声明通用 `jit_HashAggrResizeVector`。 | 中 |
| **C3** | `HashAggrJit.cpp:89-102` `kHashAggrRuntimeLinkAnchors` | 框架 TU 的链接锚点引用了 decimal 专属符号 `jit_HashAggrExtractFinalDecimalSum`，使框架编译期强依赖 decimal runtime。 | 框架锚点改用通用 runtime 符号；decimal runtime 锚点由 decimal ops TU 自持。 | 中 |
| **C4** | `HashAggrJit.cpp:1002`（配套 `GroupingSet.cpp:1177`） | 框架骨架 `if (checkInputNulls && !slot.desc.countStar)` 直接判 count 专属 flag。 | 用通用语义字段（如 `consumesInput`/`hasScalarInput`）替代 `countStar` 在框架层的判断；count 语义判定保留在 `CountOps`。 | 低 |
| **C5** | `HashAggrJit.cpp:1115-1129`（及被注释的 `signature()` `1197-1210`） | chunk 名拼接直接读 `countStar/mergeInput/decimal/kind` 等具体 flag。属较合理的元数据消费，但仍耦合 flag 名。 | 可提供 `ops->signatureSuffix(slot)` 回调由算子补充自身特征。 | 低 |

> 编排层（`GroupingSet.cpp` / `Aggregate.cpp` 的 slot/descriptor 构建）已通过虚函数 `supportsHashAggrJit`/`createHashAggrJitDescriptor` 干净解耦，无按 kind 写死映射。

---

## 2. JIT 与非 JIT 数据结构一致性（关注点②）

### 2.1 逐对结论（当前均一致）

| 聚合 | JIT | 非 JIT | 结论 |
|---|---|---|---|
| SUM | `SumOps.cpp` 单标量写 `slot.offset` | `SumAggregateBase.h` 裸标量 | ✅ 一致 |
| AVG | `AvgOps.cpp:16-23` `{double sum; int64 count}` + offsetof | `SumCount<double>` `AverageAggregateBase.h:81-84` | ✅ 一致（ROW 子列 `{sum,count}`） |
| DECIMAL SUM | `JitDecimalSumState{sum,overflow,isEmpty}` + offsetof | `DecimalSum` `DecimalSumAggregate.h:37-48` | ✅ 一致（ROW `{sum,isEmpty}`，溢出哨兵语义一致） |
| DECIMAL AVG | `JitDecimalAvgState{sum,count,overflow}` + offsetof | `LongDecimalWithOverflowState` `DecimalAggregate.h:45-82` | ✅ 一致（ROW `{sum,count}`） |
| COUNT | `CountOps.cpp` 单 i64，结果永不 null | `CountAggregate.cpp` 裸 int64 | ✅ 一致 |
| MIN/MAX | `MinMaxOps.cpp` 单标量，null 表「空」 | `MinMaxAggregates.cpp` 裸标量 | ✅ 一致（Int128/Bool 回退非 JIT） |

### 2.2 一致性风险点（靠人工同步维持，需重点盯）

| # | 位置 | 问题 | 风险 |
|---|---|---|---|
| **R1** ✅ **已完成** | `AvgOps.cpp:16-23` vs `AverageAggregateBase.h:81-84` | ~~`AvgAccumulatorLayout` 与 `SumCount<double>` 是两份独立定义，跨编译单元无法交叉 `static_assert(sizeof/offsetof==)`。改一处忘改另一处会静默写错 count 偏移。~~ 已抽出零依赖头 `SumCount.h` 作为唯一权威定义；JIT 端 `using AvgAccumulatorLayout = functions::aggregate::SumCount<double>`，offset 由权威结构 `offsetof` 派生，自动同步，镜像漂移消除。 | 中 |
| **R2** ✅ **已完成** | `HashAggrJitDecimalState.h:16-26` vs `DecimalSumAggregate.h:37-48` / `DecimalAggregate.h:45-82` | ~~同为镜像复制。注意 DecimalSum 是 `{sum,overflow,isEmpty}`、DecimalAvg 是 `{sum,count,overflow}`，**overflow 字段位置不同**；且 `LongDecimalWithOverflowState::serialize()` 顺序（count,overflow,sum）又与内存布局不同，极易混淆。JIT 只读内存不走 serialize，当前正确但无编译期交叉校验。~~ 已抽出零依赖头 `DecimalAccumulatorLayout.h`（`DecimalSumAccumulatorLayout`/`LongDecimalWithOverflowLayout` 两个 POD 布局基类）；`DecimalSum`/`LongDecimalWithOverflowState` 继承之（只加方法、不加数据成员，保持 standard-layout），JIT 端 `using JitDecimalSumState/JitDecimalAvgState` 别名同一布局基类。布局自动同步，4 处 `static_assert(is_standard_layout_v)` 兜底防派生类加字段。 | 中 |
| **R3** | `HashAggrDecimalRuntime.cpp:29-110` | 4 个 runtime helper 逐行复制了非 JIT 的 `computeFinalValue`/`computeAvg`/`adjustSumForOverflow`/`rescaleWithRoundUp` 及常量（`kCountPrecision=20` 等）。属行为复制，非 JIT 改 decimal 语义需同步两处。 | 中 |
| **R4** | `AvgOps.cpp:114-129` | 全 null group partial extract 输出 `(0,0)` 且 top-level 非 null，对齐的是 sparksql 重载版 `AverageAggregate.cpp:112-132`（非 lib 基类版）。需确认 JIT 仅用于 sparksql 路径。 | 低 |
| **R5** | `AvgOps.cpp:42-70`/`132` | avg 的最终 null 实际靠 `count==0` 判定，accumulator null byte 对 avg 不参与结果 null，存在但冗余，易误读。 | 低 |
| **R6** | `MinMaxOps.cpp:74-79`/`SumOps.cpp:51-55`/`AvgOps.cpp:100-103` | `canCompile*Extract` 用 accumulatorKind 白名单回退非 JIT，正确但靠人工维护；新增类型忘更新可能误走 JIT。 | 低 |

---

## 3. 代码坏味道（关注点①）

### 3.1 高严重度

| # | 位置 | 问题 | 建议 |
|---|---|---|---|
| **S1** | `HashAggrJit.cpp:906-925`/`953-978`/`1047-1067` | 三个 gen 函数的 LLVM 函数原型构造、entry/loop/end BB、`numRows<=0` guard、行循环 PHI、groupAddr/group 三段骨架几乎逐字重复 3 份。 | 抽取 `beginGroupLoop()/endGroupLoop()` 公共辅助返回 `{Function*, loop/end BB, PHI* row, group}`；`i8PtrTy/i32Ty/voidTy` 收进 `JitTypes` 缓存。 |
| **S2** | `SumOps.cpp:14-27`/`57-69` vs `MinMaxOps.cpp:14-25`/`81-93`（Avg init 前半段同） | Sum 与 MinMax 的 init（setNull+存0）与 extract（load+isNull+write）逐行相同。 | 抽 `compileZeroInitNullableAccumulator()` 与 `compileSimpleNullableExtract()` 复用。 |
| **S3** ✅ **已完成** | `HashAggrJitDecimalState.h:16-26` | ~~JIT decimal state 与非 JIT accumulator 重复定义，靠 `static_assert(standard_layout)` 无法保证与原结构字段顺序/对齐一致。~~ 已抽出零依赖布局基类（`DecimalAccumulatorLayout.h`），JIT 端用 `using` 别名复用，非 JIT 结构继承同一基类，布局单一权威来源。 | `using` 复用原结构，或加 `static_assert(sizeof/offsetof==)` 钉死并注释「布局必须与 X 同步」。 |

### 3.2 中严重度

| # | 位置 | 问题 | 建议 |
|---|---|---|---|
| **S4** | `HashAggrJitTypes.h:141` + `HashAggrJit.cpp:1197-1210` | 被注释掉的死代码 `signature()`；`<fmt/format.h>`(`:15`) 仅服务这段死代码；逻辑与 chunk 名拼接重合。 | 删死代码或与 chunk 名拼接合并为真函数，移除多余 include。 |
| **S5** | `HashAggrJit.h:29-35` AddFn 的 `nextBlock` 参数 | 6/8 ops 实现未用（匿名 `BasicBlock*`），仅 decimal 用；框架调用后又无条件 `CreateBr(nextBlock)`（`:1028`），控制流职责模糊。 | 将「分支到 nextBlock」职责收归框架，decimal overflow 分流改用局部 if/PHI，从签名删除 `nextBlock`。 |
| **S6** | `SumOps.cpp:51`/`AvgOps.cpp:100`/`MinMaxOps.cpp:74`/`CountOps.cpp:69`/`DecimalAvgOps.cpp:123` | canExtract 第二参数有的匿名有的命名 `partialOutput` 却不用，风格不一。 | 统一：不用就一律匿名，typedef 处注释语义。 |
| **S7** | `HashAggrJit.cpp:692-716` `ScalarOutputAdapterCodegen::write` | `kind` 不支持时静默 no-op（既不写也不报错），与 `RowOutputAdapterCodegen::writeField`(`:762` 用 BOLT_CHECK) 不一致。 | 补 `else BOLT_UNSUPPORTED(...)`。 |
| **S8** | `HashAggrJit.cpp:1115-1129` | 超长 ostringstream 拼接函数名，单字符 flag（`s/x`,`g/r`,`d/n`）无注释，可维护性低。 | 抽 `appendSlotSignature(out, slot)` 并加注释。 |
| **S9** | gen 用 `return false`（`:937-939,1019-1026,1092-1094`）；适配器用 `BOLT_UNSUPPORTED`（`:583,590,610,723,759`）；`writeField` 用 `BOLT_CHECK`（`:767`） | 同模块对「不支持/非法状态」三种处理方式混用。 | 明确契约：可降级→bool，编程错误/不变量破坏→BOLT_CHECK，头注释写清。 |

### 3.3 低严重度

| # | 位置 | 问题 | 建议 |
|---|---|---|---|
| **S10** | `HashAggrJit.cpp:995-1001`/`1085-1091` | 循环内每 slot `make_unique` 适配器（轻量值类型无需堆分配+虚表）。 | 用 `std::variant<...>` 或栈对象+基类引用。 |
| **S11** | `HashAggrJit.cpp:988`/`1077` | `for (auto i=0; i<slots.size(); ++i)` signed/unsigned 比较，`i` 又用于 `CreateConstInBoundsGEP1_64`（需 uint64）。 | 改 `size_t i`。 |
| **S12** | `DecimalSumOps.cpp:50` 等 `auto& b=...`；`CountOps.cpp:25` `addInc`；`HashAggrJit.cpp` `fn`/`argIt` | 缩写晦涩、风格不统一。 | 统一全称 `builder`/`function`。 |
| **S13** | `HashAggrJit.cpp:805-815` 传 `scale`/`longDecimal`，runtime `HashAggrDecimalRuntime.cpp:125-126,152-153,186-190` 形参 `/*忽略*/` | IR 端计算并传了 runtime 不读的参数（无效计算+接口噪音）。 | 精简 helper 签名删未用参数，或注释说明预留。 |
| **S14** | `HashAggrJit.cpp:192-197`→`226-236` `loadPointerField` | 仅为排版前向声明匿名 namespace 小工具，增加跳转。 | 定义上移到首个使用者前，删前向声明。 |
| **S15** | `HashAggrJit.cpp:168`/`190`/`886`/`1156` | switch 已覆盖全 enum，末尾静默兜底返回是死代码，可能掩盖将来漏 case。 | 改 `BOLT_UNREACHABLE()`/`__builtin_unreachable()`。 |

---

## 4. 数据结构冗余（关注点④）

### 4.1 高严重度

| # | 位置 | 问题 | 建议 |
|---|---|---|---|
| **D1** | `HashAggrJitTypes.h:133-138`（`precision/scale/auxPrecision/auxScale`），且 descriptor 整体内嵌进每个 `HashAggrJitSlot`（`:151`） | Count/Sum/Avg/MinMax 永不读这 4 个 int32，却随每 slot 复制 16 字节死数据，按值拷进 slot 放大冗余。 | 移到独立 `DecimalExtractParams`，descriptor 仅在 decimal 时持 `optional`/指针；或 union/variant 按 kind 存。 |
| **D2** ✅ **已完成** | `HashAggrJitDecimalState.h:16-26` vs `DecimalSumAggregate.h:37` / `DecimalAggregate.h:45` | ~~同一累加器内存布局定义两遍，字节兼容却无强约束（同 S3/R2）。~~ 已抽出共享 POD 布局基类（`DecimalAccumulatorLayout.h`），非 JIT 结构继承、JIT 端 `using` 别名，布局单一来源不再重复定义。 | 复用或 `static_assert` 钉死 offsetof/sizeof。 |
| **D3** | `HashAggrJitTypes.h:126`（`descriptor.countStar`）与 `:87-89`（`PlanContext::isCountStar()` 派生自 `isRawInput && inputCount==0`） | count(*) 信息一处派生一处落字段，两套来源易漂移。 | 单一权威来源，descriptor 的 `countStar` 由 plan 阶段一次写入并注释「唯一来源」。 |

### 4.2 中严重度

| # | 位置 | 问题 | 建议 |
|---|---|---|---|
| **D4** | `HashAggrJitTypes.h:92-98` enum `HashAggrJitKind` 与 `HashAggrJit.h:43` `ops->id` 字符串 | 算子身份 enum + 字符串双重标识；`kind` 实际仅 MinMax 区分用到（`MinMaxOps.cpp:48,61`）+ chunk 命名，Sum/Avg/Count 的 kind 与 ops 冗余。 | 只留 `ops` 指针给 MinMax 加 `isMin` 标志或拆两个 ops；或去 id 字符串改 kind 派生名，二选一。 |
| **D5** | `HashAggrJitTypes.h:128`（`decimal`）、`:129-130`（`inputShape/outputShape`） | `decimal` 与「ops 是否 Decimal*」一一对应；shape 与适配器选择（`HashAggrJit.cpp:888-894`）一一对应，属派生型冗余。 | ops 表暴露 `isDecimal`/`defaultShape` 后可去字段；否则注释「与 ops 绑定，prepare 填充」。 |
| **D6** | `HashAggrJitTypes.h:56`/`68` union 两变体各存 `void* vector` | scalar 与 row 输出变体都放一个语义相同的顶层 `vector` 字段。 | 把 `vector` 提到 union 外公共头部。 |
| **D7** | `AvgOps.cpp:16-19`（局部）vs `HashAggrJitDecimalState.h`（共享头）；Sum/MinMax 裸标量是隐式约定无 struct | accumulator layout 存放位置与表达方式不一致。 | 统一：都用具名 struct+offsetof 或都注释化。 |

### 4.3 低严重度

| # | 位置 | 问题 | 建议 |
|---|---|---|---|
| **D8** | `HashAggrJitTypes.h:100-109`；`HashAggrJit.cpp:152-154`/`176-189` | `Bool` 在 llvmType 等价 Int8，仅少数处特判，枚举语义重叠。 | 评估改 Int8+`isBool` 标志，或注释说明差异点。 |
| **D9** | `HashAggrJitTypes.h:144-152` | slot 与 desc **未**重复携带 offset/null（已确认）；但 `desc` 按值内嵌使多 slot 拷贝整份 descriptor（与 D1 叠加）。 | 若 descriptor 可共享，slot 改持 `const HashAggrJitDescriptor*` 减少拷贝与死字段复制。 |

---

## 5. 建议的优化顺序

1. **C1** ✅ **已完成**：decimal IR 生成已下沉到 ops（`emitDecimalAddWithOverflow`/`emitDecimalSumExtract` 定义于 `DecimalSumOps.cpp`，`emitDecimalAvgExtract` 定义于 `DecimalAvgOps.cpp`，声明在新增的 `ops/DecimalOps.h`；框架类 `HashAggrJitCodegen` 不再持有任何 "Decimal" 方法）。
2. **S1 + S2**：消除三大 gen 骨架重复、合并 Sum/MinMax init/extract。
3. **S3/D2 + R2** ✅ **已完成**：decimal state 双定义已改为继承共享 POD 布局基类（`DecimalAccumulatorLayout.h`）+ JIT `using` 别名复用，布局单一权威来源（同时降一致性风险与冗余）。
4. **D1/D9**：descriptor decimal 死字段拆出 + slot 改持指针。
5. **C2/C3**：decimal runtime 声明与链接锚点下沉。
6. **R1** ✅ **已完成** / **R3**：R1（Avg layout）已通过抽出 `SumCount.h` + JIT `using` 复用消除镜像；R3（decimal runtime 逻辑复制）待加交叉校验/同步注释。
7. 其余坏味道（S4–S15）、冗余（D3–D8）按批次清理。

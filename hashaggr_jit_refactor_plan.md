# Bolt Hash Aggregation JIT 框架重构落地方案

> 目标读者：AI/工程师，按本文档执行即可完成 `hash_aggr_jit` 分支当前框架的重构落地。
> 适用版本：`dp/bolt @ hash_aggr_jit` 分支（基于 commit `9a65fd2` 之后）。
> 本方案只描述 **JIT 框架层**重构，不涉及非 JIT codepath。

---

## 0. TL;DR

把当前 `HashAggrJitDecodedInput / HashAggrJitOutput / per-aggregate codegen` 这一套耦合实现，重构为 **三层正交架构**：

```
┌──────────────────┐   IRRow    ┌─────────────┐   IRRow    ┌────────────────┐
│  InputAdapter    │ ─────────▶ │  GroupOps   │ ─────────▶ │ OutputAdapter  │
│  (Vector → IR)   │            │ (IR ↔ Group)│            │ (IR → Vector)  │
└──────────────────┘            └─────────────┘            └────────────────┘
```

三层之间的唯一传输格式是 **LLVM First-Class Aggregate** 类型：

```
IRRow_t = llvm::StructType::get(value_type, i1_ty)
        = { T, i1 }   // T 由 aggregate 自己决定，可以是复合类型
```

`is_null` 永远在第二个字段，框架统一处理；`value_type` 内部结构对框架透明。

---

## 1. 当前问题（背景）

落地前必须理解这些已存在的痛点，重构必须**逐项消除**。

### 1.1 数据结构无通用性

```cpp
// HashAggrJit.h —— 反例
struct HashAggrJitDecodedInput {
  const void* data;
  const uint64_t* nulls;
  // ... 写死了若干字段，新增 aggregate 类型就要扩字段
};
struct HashAggrJitOutput { /* 同上 */ };
```

- **病症**：每加一种聚合 / 一种 vector encoding，就要改这两个结构 + 改 IR 的 hardcoded byte offset。
- **影响**：ABI 双向耦合（C++ struct ↔ IR offset），任何字段重排都是坑。

### 1.2 Vector ↔ IR 与 IR ↔ Group 两段逻辑混在一起

每个 `XxxAggregate::codegenAddDense / codegenExtract` 同时做：
1. 从输入 vector decode 出值
2. 在 IR 里做累加 / 比较
3. 把结果按 group 内 memory layout 写回

→ 三件事完全不正交，维护成本爆炸；且每个 aggregate 都要重新写 vector decoding 逻辑。

### 1.3 复合 value 类型（avg）特殊化

avg intermediate 当前在多处直接写成三元组 `{f64 sum, i64 count, i1 is_null}`，把 null 处理跟 value 内部结构耦合在一起，框架 helper 无法复用。

---

## 2. 目标架构

### 2.1 核心抽象：`IRRow`

**契约**：

```cpp
// 框架级 invariant —— 所有 aggregate 共用
IRRow_t(value_type) := llvm::StructType::get(value_type, i1Ty)
//                                            ^^^^^^^^^^  ^^^^^
//                                            field 0     field 1 (is_null)
```

**关键决策（已与作者确认）**：当 `value_type` 本身是复合类型（如 avg 的 `{double sum, i64 count}`），**采用嵌套** `{{double, i64}, i1}`，不采用平铺 `{double, i64, i1}`。

理由（简版，详细对比见 §6）：
- 嵌套保持 `IRRow = {T, i1}` 不变量，框架 helper 完全通用；
- 平铺让 framework 必须知道 T 内部 field 数量，破坏抽象；
- 二者 memory layout 完全相同（24B），lowering 后寄存器分配完全一致，**性能零差异**；
- 未来 stddev / HLL / array_agg 等复合 value 聚合都能复用同一套框架。

### 2.2 三层职责

| 层 | 输入 | 输出 | 不该做 |
|----|------|------|--------|
| **InputAdapter** | `BaseVector*` + row index (IR) | `IRRow`（in register） | 不感知 group memory |
| **GroupOps** | `IRRow` + `group ptr`（IR） | 写回 group / 产出新 `IRRow` | 不感知 vector encoding |
| **OutputAdapter** | `IRRow` + `BaseVector*` + row index | 写回 vector | 不感知 group memory |

每一层都对其它两层透明 —— 通过 IRRow 的标准接口（见 §3）通信。

### 2.3 调用链对应关系

| 算子方法 | 三层调用链 |
|----------|-----------|
| `addRawInput` | `InputAdapter::read(rawVec, i)` → `GroupOps::accumulate(group, IRRow)` |
| `addIntermediateResults` | `InputAdapter::read(intVec, i)` → `GroupOps::merge(group, IRRow)` |
| `extractIntermediateResults` | `GroupOps::loadIntermediate(group)` → `OutputAdapter::write(intVec, i, IRRow)` |
| `extractResults` | `GroupOps::finalize(group)` → `OutputAdapter::write(finalVec, i, IRRow)` |
| `initGroup` | `GroupOps::init(group)` |

---

## 3. 框架层 API（必须实现）

新增文件：`velox/exec/jit/IRRow.h`、`velox/exec/jit/InputAdapter.h`、`velox/exec/jit/GroupOps.h`、`velox/exec/jit/OutputAdapter.h`（路径按 bolt 现有 jit 目录调整）。

### 3.1 `IRRow` —— 唯一传输格式

```cpp
class IRRow {
 public:
  // 类型构造：value_type 由 aggregate 决定
  static llvm::StructType* getType(llvm::IRBuilder<>& b, llvm::Type* value_type) {
    return llvm::StructType::get(value_type, b.getInt1Ty());
  }

  // ---- 读 ----
  static llvm::Value* getValue(llvm::IRBuilder<>& b, llvm::Value* row) {
    return b.CreateExtractValue(row, {0});
  }
  static llvm::Value* getIsNull(llvm::IRBuilder<>& b, llvm::Value* row) {
    return b.CreateExtractValue(row, {1});
  }

  // ---- 写 ----
  static llvm::Value* pack(llvm::IRBuilder<>& b,
                           llvm::Value* val,
                           llvm::Value* is_null) {
    auto* ty  = llvm::StructType::get(val->getType(), is_null->getType());
    auto* tmp = b.CreateInsertValue(llvm::UndefValue::get(ty), val, {0});
    return b.CreateInsertValue(tmp, is_null, {1});
  }

  static llvm::Value* withValue(llvm::IRBuilder<>& b,
                                llvm::Value* row,
                                llvm::Value* val) {
    return b.CreateInsertValue(row, val, {0});
  }
  static llvm::Value* withIsNull(llvm::IRBuilder<>& b,
                                 llvm::Value* row,
                                 llvm::Value* is_null) {
    return b.CreateInsertValue(row, is_null, {1});
  }

  // ---- 复合 value 的二级访问：仅在 GroupOps 内部使用 ----
  static llvm::Value* getValueField(llvm::IRBuilder<>& b,
                                    llvm::Value* row,
                                    unsigned idx) {
    return b.CreateExtractValue(row, {0, idx}); // 注意：嵌套 GEP
  }
};
```

**强约束**：除了 `IRRow` 这套 helper 之外，**任何代码不得**直接对 IRRow struct 做 `extractvalue` / `insertvalue` —— 一旦发现就是抽象泄漏。

### 3.2 `InputAdapter` —— 规范化输入描述 → IRRow

```cpp
class InputAdapter {
 public:
  virtual ~InputAdapter() = default;

  // 在 codegen 阶段调用，返回 IR 类型（必须等于对应 aggregate 的 IRRow_t）
  virtual llvm::StructType* irRowType(llvm::IRBuilder<>& b) const = 0;

  // 在 IRBuilder 当前位置生成读取代码：从 vector + index 读出一个 IRRow
  virtual llvm::Value* read(llvm::IRBuilder<>& b,
                            llvm::Value* vector_ctx,
                            llvm::Value* row_idx) const = 0;
};
```

**关键修正**：这里的 `InputAdapter` **不能**按原始 vector encoding（flat / constant / dictionary）拆成不同 JIT 实现。

原因是：同一个 compiled chunk 会反复运行在不同 batch 上，而 batch 的原始 encoding 可以变化。若按原始
encoding 生成不同 IR，则 JIT module cache key 会被 batch 形态污染，代码无法收敛，甚至会退化成“按批次特化并反复编译”。

因此，正确边界应当是：

```text
原始 Vector(flat/constant/dictionary/...)
        │
        ▼
GroupingSet / DecodedVector 先做批次级规范化
        │
        ▼
Canonical decoded descriptor
  { values, indices, nulls, decodedVector, rowField*... }
        │
        ▼
InputAdapter 只针对“规范化后的运行时描述”生成 IR
```

也就是说：

- flat / constant / dictionary 的差异，应该在 **JIT 之前** 被 `DecodedVector` + runtime descriptor 吸收；
- JIT 内的 `InputAdapter` 面向的是**稳定 ABI**，而不是每个 batch 的原始 encoding；
- 这样生成出来的 IR 才能在不同 batch 上复用并保持收敛。

**实现一览**（最少需要这些 adapter，它们对应“规范化后的输入形态”，而不是原始 encoding）：

| Adapter | 处理的规范化形态 | 关键 IR 行为 |
|---------|------------------|--------------|
| `DecodedScalarInputAdapter<T>` | 标量输入：`values + indices + nulls` | `index = indices[row]`；`gep + load` 数据；`bit test` top-level nulls；pack 成 IRRow |
| `DecodedRowInputAdapter` | ROW intermediate：`rowField* + decodedVector(fallback)` | 优先走 field raw pointers/nulls；必要时回退 row-field helper；在 IR 里构造嵌套 IRRow |
| `CountStarInputAdapter` | 无实参输入 | 直接产出固定非空 IRRow / 或由 GroupOps 特判 |

> 每个 adapter **只负责自己**对应的“规范化输入 contract”到 IRRow 的转换，不涉及任何聚合语义。
>
> 特别注意：`DecodedScalarInputAdapter<T>` 生成的 IR 在 flat / constant / dictionary batch 上应完全相同；不同 batch
> 只通过 `indices/nulls/values` 的运行时内容体现差异，而不改变 IR 形状。

### 3.3 `GroupOps` —— IRRow ↔ Group

**关键修正**：`GroupOps` 在 bolt 当前实现里，**不应该**被设计成“拥有 group layout / group size / group align”的抽象。

当前事实是：

- group memory 由 `RowContainer + AggregateInfo + accumulator layout` 共同决定；
- JIT 侧真正拿到的是 `group ptr + HashAggrJitSlot`；
- 访问状态依赖 `slot.offset / slot.nullByte / slot.nullMask`，以及像 `JitAvgState` / `JitDecimal*State`
  这样的现有 state struct offset；
- 当前 `HashAggrJitOps` 也是围绕这个 contract 工作，而不是自己管理 group allocation。

因此，更贴近 bolt 现状的 `GroupOps` 应该是：**“在既有 slot/layout 之上生成 group state 读写 IR 的薄层 policy”**，而不是一个重新定义 group 存储协议的 owner。

```cpp
class GroupOps {
 public:
  virtual ~GroupOps() = default;

  // 该聚合的 intermediate value type（不含 is_null，框架自动包一层）
  virtual llvm::Type* intermediateValueType(llvm::IRBuilder<>& b) const = 0;
  virtual llvm::Type* finalValueType(llvm::IRBuilder<>& b) const = 0;

  // ---- codegen hooks ----
  // slot 提供当前 aggregate 在 group row 中的 offset/null-bit 等元数据。
  virtual void init(HashAggrJitCodegen& codegen,
                    llvm::Value* group,
                    const HashAggrJitSlot& slot) const = 0;

  // 用 raw input 的 IRRow 累加进 group（对应当前 addRawInput）。
  virtual void accumulate(HashAggrJitCodegen& codegen,
                          llvm::Value* group,
                          llvm::Value* input_irrow,
                          const HashAggrJitSlot& slot,
                          llvm::BasicBlock* nextBlock) const = 0;

  // 用 partial / intermediate 的 IRRow 合并进 group（对应当前 addIntermediateResults）。
  virtual void merge(HashAggrJitCodegen& codegen,
                     llvm::Value* group,
                     llvm::Value* intermediate_irrow,
                     const HashAggrJitSlot& slot,
                     llvm::BasicBlock* nextBlock) const = 0;

  // 从 group 读出 intermediate IRRow（extractIntermediateResults）
  virtual llvm::Value* loadIntermediate(HashAggrJitCodegen& codegen,
                                        llvm::Value* group,
                                        const HashAggrJitSlot& slot) const = 0;

  // 从 group 读出 final IRRow（extractResults）
  virtual llvm::Value* finalize(HashAggrJitCodegen& codegen,
                                llvm::Value* group,
                                const HashAggrJitSlot& slot) const = 0;

  virtual bool canExtract(const HashAggrJitSlot& slot,
                          bool partialOutput) const = 0;
};
```

**关键约束**：
1. `loadIntermediate` 返回的 IRRow 类型 = `IRRow::getType(intermediateValueType())`；`finalize` 返回 = `IRRow::getType(finalValueType())`。
2. group 内 memory layout **不是 `GroupOps` 自己分配/注册**的；它依旧来源于现有 accumulator/state layout，`GroupOps` 只是通过 `slot + state field offset` 去访问。
3. 第一阶段 `GroupOps` 可以是当前 `HashAggrJitOps` 的**薄 facade**：先把“状态读写逻辑”从 aggregate ops 中理顺，不要求第一步就重写整个 JIT chunk 生成框架。
4. **null 处理统一在这一层完成**：`accumulate / merge` 必须显式处理 `IRRow::getIsNull(input)`，框架不再依赖任何外部状态。
5. `nextBlock` 仍作为参数保留，是为了兼容当前 `genAddDenseIR(...)` 的控制流拼装方式；不要为了追求接口漂亮而强行重写外层 loop/branch 骨架。

### 3.3.1 与当前 `HashAggrJitOps` 的映射

为了降低迁移风险，建议第一阶段直接保持与现有 `HashAggrJitOps` 一一对应：

| 当前接口 | 收敛后的职责 |
|----------|--------------|
| `initGroup` | `GroupOps::init` |
| `addRawInput` | `InputAdapter::read(raw)` → `GroupOps::accumulate` |
| `addIntermediateResults` | `InputAdapter::read(intermediate)` → `GroupOps::merge` |
| `canExtract` | `GroupOps::canExtract` |
| `extract` | `GroupOps::loadIntermediate/finalize` → `OutputAdapter::write` |

也就是说，**第一步不是删掉 `HashAggrJitOps`，而是让它退化为一个桥接层**：

- 对外仍维持当前 JIT chunk 代码生成入口；
- 对内逐步把输入读取 / group 状态访问 / 输出写回转发到新三层；
- 等所有 aggregate 都迁完后，再决定是否彻底折叠旧表结构。

### 3.4 `OutputAdapter` —— IRRow → Vector

```cpp
class OutputAdapter {
 public:
  virtual ~OutputAdapter() = default;

  virtual llvm::StructType* irRowType(llvm::IRBuilder<>& b) const = 0;

  // 把 IRRow 写入 vector[row_idx]
  virtual void write(llvm::IRBuilder<>& b,
                     llvm::Value* vector_ctx,
                     llvm::Value* row_idx,
                     llvm::Value* irrow) const = 0;
};
```

输出端通常只需要 `FlatOutputAdapter<T>` 和 `RowOutputAdapter`（写复合 intermediate）。

---

## 4. 各聚合落地示例

### 4.1 `sum<int64>` / `sum<double>`（最简单）

```cpp
class SumGroupOps : public GroupOps {
  llvm::Type* intermediateValueType(IRBuilder& b) const override { return b.getInt64Ty(); }
  llvm::Type* finalValueType(IRBuilder& b)        const override { return b.getInt64Ty(); }

  void init(HashAggrJitCodegen& codegen,
            Value* group,
            const HashAggrJitSlot& slot) const override {
    codegen.setAccumulatorNull(group, slot);
    codegen.storeValue(group, codegen.builder().getInt64Ty(), slot.offset,
                       codegen.builder().getInt64(0));
  }

  void accumulate(HashAggrJitCodegen& codegen,
                  Value* group,
                  Value* in,
                  const HashAggrJitSlot& slot,
                  BasicBlock*) const override {
    auto& b = codegen.builder();
    auto* in_null = IRRow::getIsNull(b, in);
    auto* in_val  = IRRow::getValue(b, in);
    // if (!in_null) { sum += in_val; is_null = false; }
    BasicBlock *if_t = ..., *cont = ...;
    b.CreateCondBr(b.CreateNot(in_null), if_t, cont);
    b.SetInsertPoint(if_t);
    auto* old = codegen.loadValue(group, b.getInt64Ty(), slot.offset);
    codegen.storeValue(group, b.getInt64Ty(), slot.offset, b.CreateAdd(old, in_val));
    codegen.clearAccumulatorNull(group, slot);
    b.CreateBr(cont);
    b.SetInsertPoint(cont);
  }

  void merge(HashAggrJitCodegen& codegen,
             Value* group,
             Value* in,
             const HashAggrJitSlot& slot,
             BasicBlock* next) const override {
    accumulate(codegen, group, in, slot, next);
  }

  Value* loadIntermediate(HashAggrJitCodegen& codegen,
                          Value* group,
                          const HashAggrJitSlot& slot) const override {
    auto& b = codegen.builder();
    return IRRow::pack(
        b,
        codegen.loadValue(group, b.getInt64Ty(), slot.offset),
        codegen.isAccumulatorNull(group, slot));
  }
  Value* finalize(HashAggrJitCodegen& codegen,
                  Value* group,
                  const HashAggrJitSlot& slot) const override {
    return loadIntermediate(codegen, group, slot);
  }

  bool canExtract(const HashAggrJitSlot&, bool) const override {
    return true;
  }
};
```

### 4.2 `avg<double>`（复合 value，**采用嵌套**）

```cpp
class AvgGroupOps : public GroupOps {
  // intermediate value = { double sum, i64 count }；is_null 由框架包外层
  llvm::Type* intermediateValueType(IRBuilder& b) const override {
    return llvm::StructType::get(b.getDoubleTy(), b.getInt64Ty());
  }
  llvm::Type* finalValueType(IRBuilder& b) const override { return b.getDoubleTy(); }

  // 注意：这里不是重新定义 group layout，而是复用现有 accumulator/state layout。
  // 当前 bolt 中 avg 仍应与 JitAvgState / slot.offset / kAvgCountOffset 保持一致，
  // 避免第一阶段重构把 state ABI 一起打散。

  void init(HashAggrJitCodegen& codegen,
            Value* group,
            const HashAggrJitSlot& slot) const override {
    ... // 对齐当前 compileAvgInitGroup：setAccumulatorNull + sum/count 初始化
  }

  // raw input: IRRow_t = { double, i1 }
  void accumulate(HashAggrJitCodegen& codegen,
                  Value* group,
                  Value* in,
                  const HashAggrJitSlot& slot,
                  BasicBlock*) const override {
    auto& b = codegen.builder();
    auto* in_null = IRRow::getIsNull(b, in);
    auto* in_val  = IRRow::getValue(b, in);
    // if (!in_null) { sum += val; count += 1; }
    ...
  }

  // intermediate: IRRow_t = { {double, i64}, i1 }
  void merge(HashAggrJitCodegen& codegen,
             Value* group,
             Value* in,
             const HashAggrJitSlot& slot,
             BasicBlock* nextBlock) const override {
    auto& b = codegen.builder();
    auto* in_null = IRRow::getIsNull(b, in);
    auto* part_sum   = IRRow::getValueField(b, in, 0); // double
    auto* part_count = IRRow::getValueField(b, in, 1); // i64
    // if (!in_null) { sum += part_sum; count += part_count; }
    ...
  }

  Value* loadIntermediate(HashAggrJitCodegen& codegen,
                          Value* group,
                          const HashAggrJitSlot& slot) const override {
    auto& b = codegen.builder();
    auto* count = loadCount(b, group);
    auto* sum   = loadSum(b, group);
    auto* is_null = b.CreateICmpEQ(count, b.getInt64(0));
    // 构造嵌套 struct { double, i64 }
    auto* inner_ty = intermediateValueType(b);
    auto* inner = b.CreateInsertValue(UndefValue::get(inner_ty), sum,   {0});
    inner       = b.CreateInsertValue(inner, count, {1});
    return IRRow::pack(b, inner, is_null);
  }

  Value* finalize(HashAggrJitCodegen& codegen,
                  Value* group,
                  const HashAggrJitSlot& slot) const override {
    auto& b = codegen.builder();
    auto* count = loadCount(b, group);
    auto* sum   = loadSum(b, group);
    auto* is_null = b.CreateICmpEQ(count, b.getInt64(0));
    auto* avg = b.CreateFDiv(sum, b.CreateSIToFP(count, b.getDoubleTy()));
    return IRRow::pack(b, avg, is_null);
  }

  bool canExtract(const HashAggrJitSlot& slot, bool partialOutput) const override {
    return ...; // 第一阶段直接镜像当前 canCompileAvgExtract 语义
  }
};
```

`InputAdapter` 端只需要：
- raw 输入 → `DecodedScalarInputAdapter<double>` （IRRow = `{double, i1}`）
- intermediate 输入 → `DecodedRowInputAdapter`，读取规范化后的 `rowField* / decodedVector(fallback)`，自动构造嵌套 IRRow

### 4.3 `count`

```cpp
class CountGroupOps : public GroupOps {
  llvm::Type* intermediateValueType(IRBuilder& b) const override { return b.getInt64Ty(); }
  llvm::Type* finalValueType(IRBuilder& b)        const override { return b.getInt64Ty(); }
  // count 永远不是 null，is_null 字段恒为 false（LLVM 会优化掉）
  // ...
};
```

> 实现上应继续贴合当前 bolt：`count(*)` 的 raw-input 路径本质是 `+1`，而非真的去读取一个输入列；
> merge 路径则读取 intermediate bigint。不要为了统一接口而把 `count(*)` 硬塞进一个虚构输入列模型里。

### 4.4 `min / max`

类似 sum，把 `Add` 换成 `select(cmp, old, new)` 即可。

### 4.5 `stddev`（前瞻验证，体现可扩展性）

```cpp
llvm::Type* intermediateValueType(IRBuilder& b) const override {
  return llvm::StructType::get(b.getInt64Ty(),    // count
                               b.getDoubleTy(),   // mean
                               b.getDoubleTy());  // M2
}
// IRRow_t = { {i64, double, double}, i1 }，框架完全无需改动
```

---

## 5. 与现有代码的对接

### 5.1 删除项

| 文件 / 符号 | 处置 |
|-------------|------|
| `struct HashAggrJitDecodedInput` | 第一阶段**不删除**；先把它收敛为 canonical decoded descriptor，供 `InputAdapter` 消费；所有 aggregate 迁移完成后再决定是否改名/瘦身 |
| `struct HashAggrJitOutput` | 第一阶段**不删除**；先把它收敛为 canonical output descriptor，供 `OutputAdapter` 消费；待 extract 全迁完后再决定是否改名/瘦身 |
| 各 `XxxAggregate::codegenAddDense` 中关于 vector decoding 的代码 | 迁到 `InputAdapter` |
| 各 `XxxAggregate::codegenAddDense` 中关于 group rw 的代码 | 迁到 `GroupOps` |
| `Aggregate::numNulls_` 的更新依赖（JIT path） | 删除依赖 |
| 任何 `extractvalue` / `insertvalue` 直接对 IRRow 做的代码 | 替换成 `IRRow::*` helper |

> 额外说明：第一阶段的目标是**理顺职责边界**，不是立即改变 group row 的底层存储协议；
> `slot.offset/nullByte/nullMask` 与现有 state struct offset 仍然是合法的迁移期依赖。

### 5.2 新增项

```
velox/exec/jit/
├── IRRow.h
├── InputAdapter.h
├── input_adapters/
│   ├── DecodedScalarInputAdapter.h
│   ├── DecodedRowInputAdapter.h
│   └── CountStarInputAdapter.h
├── GroupOps.h
├── group_ops/
│   ├── SumGroupOps.{h,cpp}
│   ├── CountGroupOps.{h,cpp}
│   ├── AvgGroupOps.{h,cpp}
│   ├── MinMaxGroupOps.{h,cpp}
│   └── ...
├── OutputAdapter.h
└── output_adapters/
    ├── FlatOutputAdapter.h
    └── RowOutputAdapter.h
```

### 5.3 单测要求

新增测试 `HashAggrJitFrameworkTest.cpp`，必须覆盖：

1. `IRRow::pack/getValue/getIsNull` 在简单类型与嵌套类型上 round-trip。
2. `DecodedScalarInputAdapter` 在 flat/constant/dictionary 三种 batch encoding 上生成**同一形状 IR**，并通过不同的 `indices/nulls/values` runtime 内容得到正确结果。
3. `DecodedRowInputAdapter` 在 row-field raw fast path 与 helper fallback 两条路径上结果一致。
4. 每个 `GroupOps`：init → accumulate(若干 raw + 若干 null) → loadIntermediate → merge(到另一 group) → finalize 与 reference 实现一致。
5. **专项 null 测试**：所有输入都是 null 时，`finalize` 必须返回 `is_null = true`。
6. avg intermediate 必须验证 IRRow 的 LLVM type 字面就是 `{ {double, i64}, i1 }`（而非平铺）。

---

## 6. 嵌套 vs 平铺：决策记录（avg 等复合 value）

| 维度 | 嵌套 `{{double,i64}, i1}` ✅ | 平铺 `{double, i64, i1}` ❌ |
|------|----------------------------|---------------------------|
| `IRRow = {T, i1}` invariant | 保持 | 破坏 |
| `IRRow::getValue / getIsNull` 是否通用 | 是（`{0}` / `{1}`） | 否，avg 要 special case |
| 框架对 T 的内部结构 | 不感知 | 必须知道 field 数 |
| 新增复合 value 聚合（stddev/HLL/...） | 0 改动 | 框架每次都要扩展 |
| Memory layout | 24B（offset 0/8/16） | 24B（offset 0/8/16），完全相同 |
| LLVM lowering 性能 | 经 SROA/InstCombine 后与平铺一致 | 与嵌套一致 |
| IR 可读性 | 略冗长（多一层 `{0,k}`） | 更短 |

**结论**：嵌套方案在抽象一致性、可扩展性上完胜，且无任何性能代价。**全部聚合统一采用嵌套布局。**

---

## 7. 落地步骤（建议 PR 顺序）

为了控制每个 MR 的 diff 体积，推荐拆 4 个 MR 提交：

| # | MR 标题 | 范围 | 依赖 |
|---|---------|------|------|
| 1 | `[jit] Introduce IRRow + canonical Input/Output descriptors + GroupOps facade` | 在现有 `HashAggrJitOps` 外围引入三层抽象与单测，不改 chunk ABI | 无 |
| 2 | `[jit] Migrate sum/count/min/max onto GroupOps + Adapter internals` | 先迁简单标量聚合，外部入口保持兼容 | #1 |
| 3 | `[jit] Migrate avg with nested intermediate IRRow` | avg 落地嵌套方案，保留现有 state layout 与 extract 语义 | #2 |
| 4 | `[jit] Migrate decimal sum/avg and optionally shrink legacy tables` | decimal 收口，并视情况瘦身旧 descriptor / ops table | #3 |

每个 MR 都要：
- 跑通现有 hash aggr e2e 测试集（重点覆盖含 null 输入的 case）。
- 跑 micro benchmark 对比，必须 ≥ 当前 `9a65fd2` 性能。
- LLVM IR dump（`-dump_ir`）肉眼检查 SROA 后是否消除了 alloca。

---

## 8. 验收标准（Definition of Done）

- [ ] `HashAggrJitDecodedInput / HashAggrJitOutput` 至少已收敛为 canonical descriptor；若仍保留，也不再继续按新聚合需求横向扩字段。
- [ ] 任何 IRRow 字段访问只能经过 `IRRow::*` helper（grep `extractvalue.*IRRow|insertvalue.*IRRow` 应为零）。
- [ ] JIT path 不再读写 `Aggregate::numNulls_`。
- [ ] avg intermediate 的 LLVM type 等于 `{{double, i64}, i1}`（单测断言）。
- [ ] 新增 stddev 或任意复合 value 聚合时，**不需要修改 InputAdapter/OutputAdapter/IRRow 等框架边界定义**；最多新增对应 `GroupOps` / state helper。
- [ ] e2e 性能不回退，TPC-H Q1（avg 重灾区）持平或提升。

---

## 9. 备注

- 本方案与上游 Velox 无 conflict —— Velox 没有 hash aggr JIT，bolt 这块是独立分叉。
- 如果未来引入 PartialFinal 优化、ROW vector 嵌套加深，IRRow 接口无需改动。
- 复合 value 聚合超过 3 层嵌套（极少见）时，建议在 `IRRow` 上提供 path-based getter（`getValueByPath({0,1,2})`），但不在本次重构范围内。

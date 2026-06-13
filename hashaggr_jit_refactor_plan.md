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

### 7.0 首个最小可实施 patch（本轮直接落地）

为了避免第一步就同时改动 chunk ABI、aggregate ops table、descriptor 字段和 benchmark 口径，首个 patch 只做**最小且可验证**的框架落点：

#### Patch-1 范围

1. 新增 `IRRow` helper（建议先放在现有 `bolt/jit/aggregation/` 目录下，而不是一开始新建整套 framework 目录）：
   - `getType`
   - `pack`
   - `getValue`
   - `getIsNull`
   - `withValue`
   - `withIsNull`
   - `getValueField`
2. 新增对应单测，至少覆盖：
   - 标量 value 的 round-trip；
   - 嵌套 value（如 `{{double, i64}, i1}`）的 round-trip；
   - `withValue / withIsNull` 的覆盖更新语义；
3. **不修改**当前 `HashAggrJitChunk` ABI；
4. **不修改** `HashAggrJitDecodedInput / HashAggrJitOutput` 结构；
5. **不迁移**任何 aggregate 到新三层，只把 `IRRow` 作为第一块可复用基建先落进去。

#### Patch-1 预期收益

- 为后续 `GroupOps::loadIntermediate/finalize` 提供统一返回协议；
- 为 avg / decimal avg 这类复合 value 的嵌套 IRRow 建立稳定 helper；
- 先把最容易验证、最不影响性能的部分单独落地，降低后续 patch 风险。

#### Patch-1 验证方式

- 编译 `bolt_thrustjit`；
- 若当前配置包含测试，则额外编译并运行 `bolt_thrustjit_test`；
- 该 patch 不应改变任何现有 hash aggr JIT 生成 IR 的行为与性能。

#### Patch-2 范围（紧接 Patch-1）

第二个最小 patch 继续保持“不改 ABI、不改 chunk 骨架”的原则，只做**标量输入读取的一层内部收口**：

1. 新增一个极薄的 `DecodedScalarInputAdapter` helper；
2. 第一阶段只提供 `readKnownNotNull(...)`：
   - 适用于当前外层控制流已经完成 top-level null 过滤后的路径；
   - 直接把 `loadDecodedValue(...)` 的结果打包成 `IRRow<T>`，并把 `is_null` 固定为 `false`；
3. 仅选择 `sum` 作为第一个接入对象，把 `SumOps.cpp` 中对标量输入的直接读取改成通过该 helper；
4. 不改 `HashAggrJitChunk`、`genAddDenseIR(...)`、`HashAggrJitDecodedInput` ABI；
5. `readNullable(...)`、`DecodedRowInputAdapter`、`GroupOps facade` 留到后续 patch。

#### Patch-2 预期收益

- 验证“InputAdapter 是内部 codegen helper，而不是按 batch encoding 分裂 ABI”的设计方向；
- 让后续 `sum/minmax/avg raw-input` 迁移时有统一入口；
- 继续保证热路径不回退：外层 null 分支与现有 tight loop 骨架保持不变。

#### Patch-2 验证方式

- 编译 `bolt_thrustjit_test`；
- 运行新增 `IRRow` / `DecodedScalarInputAdapter` 相关测试；
- 确认 `SumOps.cpp` 只是把“直接 load decoded scalar”替换成 helper，不改变现有 null 过滤与算子语义。

#### Patch-3 范围（延续 Patch-2）

第三个最小 patch 继续沿用同一迁移策略，把 `DecodedScalarInputAdapter` 的使用从 `sum` 扩展到 `min/max`：

1. 不新增 ABI；
2. 不修改 `DecodedScalarInputAdapter` 接口；
3. 仅把 `MinMaxOps.cpp` 中 raw-input 标量读取切换为 `DecodedScalarInputAdapter::readKnownNotNull(...)`；
4. 保持当前外层 null 过滤、NaN 处理和比较逻辑不变；
5. 不触碰 merge row-field 路径，不引入 nullable adapter。

#### Patch-3 预期收益

- 让 `sum/min/max` 三个最基础的标量 raw-input 聚合统一走同一条内部读取入口；
- 进一步验证“InputAdapter 是内部 codegen helper，而不是新的运行时 ABI”；
- 为后续批量迁移其它标量聚合打样。

#### Patch-3 验证方式

- 编译 `bolt_thrustjit_test`；
- 运行现有 `IRRow` / `DecodedScalarInputAdapter` 相关测试，确认基建未破坏；
- 确认 `MinMaxOps.cpp` 只替换输入读取入口，不改变 min/max 的比较、NaN 语义和 extract 逻辑。

#### Patch-4 范围（先补 nullable contract，不立刻接算子）

第四个最小 patch 只补 `DecodedScalarInputAdapter` 的 nullable contract，本轮**先落 helper 与单测，不接任何 aggregate**：

1. 新增 `DecodedScalarInputAdapter::readNullable(...)`；
2. helper 负责：
   - 读取 `nulls` 指针；
   - 在 `nulls == nullptr` 时走非空快速路径；
   - 在 `nulls != nullptr` 时按 row bit 判断是否为 null；
   - 返回 `IRRow<T>{value, is_null}`；
3. null 行上不要求读取真实 payload，允许写入 typed zero 作为占位值；
4. 本 patch **不修改** `sum/min/max/count/avg` 等 aggregate；
5. 通过一个可执行的 JIT 单测验证 nullable 语义，而不是只做类型级验证。

#### Patch-4 预期收益

- 正式建立 `DecodedScalarInputAdapter` 的 nullable 语义 contract；
- 为后续把外层 null 分支逐步内聚到 InputAdapter 提供基础；
- 先用单测把“null 行不读取真实 payload、仅传递 is_null”这件事定下来，避免后续改算子时语义摇摆。

#### Patch-4 验证方式

- 编译 `bolt_thrustjit_test`；
- 运行新增 `readNullable` JIT 语义测试：
  - `nulls == nullptr` 时返回真实 value；
  - `nulls` bit 置位时返回 `is_null = true` 对应结果；
  - 非 null 行仍返回真实 value；
- 继续跑已有 `IRRow` / `DecodedScalarInputAdapter` 基础测试，确认旧 contract 不回退。

#### Patch-5 范围（让 sum 消费 nullable IRRow，但先保留外层 null branch）

第五个最小 patch 开始让真实 aggregate 消费 `readNullable(...)` 产出的 `IRRow`，但仍然坚持“不一次性收掉外层控制流”：

1. `SumOps.cpp` 的 add/merge 路径统一先读取 `inputRow = DecodedScalarInputAdapter::readNullable(...)`；
2. `sum` 内部通过 `IRRow::getIsNull(inputRow)` 决定是否跳过累加；
3. 现有 `genAddDenseIR(...)` 的 top-level null 过滤分支**保留不动**；
4. 这意味着本 patch 的行为应与当前逻辑保持一致，只是把 `sum` 的内部消费协议收口到 nullable IRRow；
5. 本 patch 不要求立即让外层 null 分支失效或删除。

#### Patch-5 预期收益

- 第一次验证“真实 aggregate 可以消费 nullable IRRow contract”；
- 为后续是否收掉外层 null 分支提供对照基线；
- 把 `sum` 变成第一个同时兼容 known-not-null 与 nullable 读取 contract 的算子样板。

#### Patch-5 验证方式

- 编译 `bolt_thrustjit_test`；
- 运行一个最小 `sum-like` JIT 语义测试：null 行返回旧 accumulator，不为 null 时返回 `old + value`；
- 继续运行已有 `IRRow` / `DecodedScalarInputAdapter` 基础测试，确认 helper contract 不回退；
- 确认 `SumOps.cpp` 仍未修改外层 null 过滤框架，仅改变输入消费方式。

#### Patch-6 范围（让 min/max 同样消费 nullable IRRow）

第六个最小 patch 把 Patch-5 在 `sum` 上验证过的模式复制到 `min/max`：

1. `MinMaxOps.cpp` 的 update 路径改为先读取 `inputRow = DecodedScalarInputAdapter::readNullable(...)`；
2. 通过 `IRRow::getIsNull(inputRow)` 显式跳过 null 行的比较与写回；
3. 非 null 行仍执行原有 min/max 比较、NaN 处理与 accumulator null 清除逻辑；
4. 现有 `genAddDenseIR(...)` 的 top-level null 过滤分支保留不动；
5. 行为与当前实现保持一致，仅把输入消费协议收口到 nullable IRRow。

#### Patch-6 预期收益

- 让 `sum/min/max` 统一以 nullable IRRow contract 消费输入；
- 进一步验证“先收口消费协议、暂不删外层 null branch”这一渐进模式在带比较/NaN 语义的算子上同样成立；
- 为后续真正收掉外层 null 分支留出一致的算子基线。

#### Patch-6 验证方式

- 编译 `bolt_thrustjit_test`；
- 运行已有 `IRRow` / `DecodedScalarInputAdapter` / `sum-like` 测试，确认 contract 不回退；
- 确认 `MinMaxOps.cpp` 仅替换输入消费方式，不改变比较、NaN 与 extract 语义，也未触碰外层 null 框架。

#### Patch-7 范围（引入最小 FlatOutputAdapter 并让 sum extract 接入）

前面几个 patch 都在 input 端收口，本 patch 开始对称地在 output 端引入第一块 helper：

1. 新增最小 `FlatOutputAdapter`（同样是 codegen-time helper，不引入任何运行时 ABI）；
2. 只提供 `writeFromIRRow(codegen, output, row, slot, irRow)`：
   - 从 `IRRow` 取 value 与 i1 `is_null`；
   - 把 `is_null` zext 到 i8；
   - 复用现有 `emitFlatValue(...)` 写回 flat 输出；
3. 让 `SumOps.cpp` 的 extract 先用 `IRRow::pack(value, is_null)` 组装，再通过 `FlatOutputAdapter::writeFromIRRow(...)` 写回；
4. 不修改 `HashAggrJitOutput` 结构与 `genExtractIR(...)` 骨架；
5. 仅 `sum` 接入，`min/max/count/avg/decimal` 的 extract 暂不动。

#### Patch-7 预期收益

- 让 output 端也有一个与 `IRRow` 对齐的统一写回入口；
- 验证“OutputAdapter 也是内部 codegen helper，而非新 ABI”这一设计方向；
- 为后续把更多 extract 收口到 `FlatOutputAdapter` / `RowOutputAdapter` 打样。

#### Patch-7 验证方式

- 编译 `bolt_thrustjit_test`；
- 编译并运行 `bolt_aggregates_test` 的 `SumTest` 相关用例，确认 sum extract 行为未回归；
- 确认 `SumOps.cpp` 的 extract 仅改写写回入口，flat 输出语义与 null 位写入保持一致。

#### Patch-8 范围（min/max 与 count 的 extract 也接入 FlatOutputAdapter）

继续把 output 端收口扩展到其余标量聚合：

1. `MinMaxOps.cpp` 的 extract 改为：`IRRow::pack(value, isAccumulatorNull)` → `FlatOutputAdapter::writeFromIRRow(...)`；
2. `CountOps.cpp` 的 extract 改为：`IRRow::pack(value, false)` → `FlatOutputAdapter::writeFromIRRow(...)`（count 永不为 null）；
3. 不修改 `HashAggrJitOutput` 结构与 `genExtractIR(...)` 骨架；
4. 行为与当前实现保持一致，只把写回入口统一到 `FlatOutputAdapter`。

#### Patch-8 预期收益

- 让 `sum/min/max/count` 四个标量聚合的 extract 全部走统一 output 入口；
- 进一步压实“OutputAdapter 是内部 codegen helper”的方向；
- 为后续 partial avg / decimal 等复杂 extract 的 RowOutputAdapter 收口铺路。

#### Patch-8 验证方式

- 编译 `bolt_thrustjit_test`、`bolt_aggregates_test`；
- 运行 `CountAggregationTest` 与 `SumTest` 相关用例，确认 extract 未引入新回归；
- 已知 `MinMaxTest` 三个 JIT 对照用例失败，本轮先忽略，仅确认未新增其它失败。

#### Patch-9 范围（avg 的 final extract 接入 FlatOutputAdapter）

avg 的 final extract 本质就是把 `avg = sum / count` 写回一个 flat double，与 sum/min/max 同形态，因此先收口 final 分支：

1. `AvgOps.cpp` 的 `compileAvgExtract` 在 `partialOutput == false` 分支改为：`IRRow::pack(avg, is_null)` → `FlatOutputAdapter::writeFromIRRow(...)`；
2. partial avg 的 ROW 输出（`emitPartialAvgResult`）暂不动，留待后续 `RowOutputAdapter`；
3. 不修改 `HashAggrJitOutput` 结构与 `genExtractIR(...)` 骨架；
4. final avg 的 `count == 0 -> null`、divide 语义保持不变。

#### Patch-9 预期收益

- 让 sum/min/max/count/avg(final) 的 flat extract 全部走统一 output 入口；
- 把 partial（ROW）与 final（flat）两类 output 路径的边界显式化，为 `RowOutputAdapter` 铺路。

#### Patch-9 验证方式

- 编译 `bolt_thrustjit_test`、`bolt_aggregates_test`；
- 运行 `AverageAggregationTest` 相关用例，确认 final avg extract 行为未回归；
- 确认 `AvgOps.cpp` 仅改写 final 分支写回入口，partial ROW 输出与 divide/null 语义不变。

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

---

## 10. InputAdapter 虚接口重构设计（Approach-1 落地稿）

这一章收敛“最终想要的效果”——**真正建立 `InputAdapter -> GroupOps -> OutputAdapter` 三层正交骨架，并最终删除 `HashAggrJitDecodedInput`**，而不是继续在旧 descriptor 上横向打补丁。

### 10.1 目标边界

本轮 InputAdapter 重构必须同时满足：

1. `InputAdapter` 提供**虚函数接口**；
2. adapter 在**构造时直接接受 vector 输入**，而不是接受 `HashAggrJitDecodedInput` 这类中间拼装物；
3. 第一层实现只分两类：
   - `ScalarInputAdapter`
   - `RowInputAdapter`
4. JIT IR **不能按 flat / constant / dictionary 三种 encoding 分叉生成**；输入 encoding 差异必须在 adapter 内部先被吸收、收敛；
5. 热路径性能不能回退：不能把“每行一次 helper / 每行一次虚调用”重新引回 add-dense loop；
6. 完成后可以删除 `HashAggrJitDecodedInput`，后续新增聚合也不允许再给它加字段。

### 10.2 分层职责

#### A. InputAdapter：只负责“把 vector 解释成 IRRow 输入契约”

InputAdapter 的职责是：

- 接受 batch 内真实的 `BaseVector` / `RowVector` 输入；
- 在 adapter 内部完成 decode / flatten / indices/nulls 收敛；
- 对 JIT 暴露**稳定、encoding 无关**的 runtime payload；
- 对 codegen 暴露“如何从该 payload 读出 `IRRow<T>`”的统一接口。

它**不负责**：

- 聚合 state layout；
- add / merge / extract 语义；
- 输出 vector 写回；
- 聚合专有字段语义（例如 avg 的 `sum/count`、decimal 的 `isEmpty`）。

#### B. GroupOps：只负责“消费/产生 IRRow”

GroupOps 只看：

- 输入：`IRRow<T>` 或嵌套 `IRRow<struct>`
- 状态：`group + slot.offset`
- 输出：`IRRow<U>`

也就是说，`sum/min/max/count/avg/decimal` 的差异只体现在各自 ops/state helper 中；
**GroupOps 不拥有 InputAdapter / OutputAdapter 的 ABI，也不拥有 group layout 定义权**。

#### C. OutputAdapter：只负责“把 IRRow 写回结果 vector”

OutputAdapter 只做两件事：

- flat output：写一个标量 `IRRow<T>`；
- row output：把 `IRRow<struct{...}>` 的每个 child field 和 top-level null 写回。

它不应理解“这是 avg 的 2-field row”或“这是 decimal sum 的 3-field row”；
字段个数/字段类型来自 `IRRow` 的 payload type，本身不携带聚合语义。

### 10.3 运行时对象模型

最终运行时不再构造 `HashAggrJitDecodedInput`，而是构造 adapter 对象：

```cpp
class InputAdapter {
 public:
  virtual ~InputAdapter() = default;

  // 供 batch 准备阶段调用；完成 decode / flatten / child adapter 建立。
  virtual void prepare() = 0;

  // 返回稳定的、可传入 JIT add_dense 的 runtime payload。
  virtual const void* runtime() const = 0;

  // 返回与该 adapter 对应的 codegen 节点。
  virtual const InputAdapterCodegen& codegen() const = 0;
};

class ScalarInputAdapter final : public InputAdapter { ... };
class RowInputAdapter final : public InputAdapter { ... };
```

这里的关键点是：

- **虚函数只发生在 batch 准备阶段**；
- JIT 热循环不做 virtual dispatch；
- JIT add-dense 看到的仍然是 `char**` / `void**` 风格的 runtime payload 数组，只是 payload 的拥有者从旧 struct 变成了 adapter；
- 因此性能上仍保持“每行直接 load 指针/indices/nulls”的 fast path。

### 10.4 Runtime payload 形状

为了替换 `HashAggrJitDecodedInput`，需要把“adapter 对象”和“JIT 可直接 load 的 POD payload”解耦：

```cpp
struct ScalarInputRuntime {
  const void* values;
  const int32_t* indices;
  const uint64_t* nulls;
};

struct RowInputRuntime {
  const uint64_t* nulls;
  const ScalarInputRuntime* const* children; // scalar child runtimes
  int32_t numChildren;
};
```

约束如下：

- `ScalarInputRuntime` 对应今天 canonical decoded descriptor 的标量子集；
- `RowInputRuntime` 不再内嵌 `rowField0Values/rowField1Values/...` 这种聚合专有字段；
- `RowInputRuntime` 也不再保留 `indices`：row 自身不再承载 dictionary/constant 等 wrapping，若 child 仍需索引映射，应下沉到 child 自己的 scalar runtime；
- 当前阶段 `RowInputRuntime.children[i]` 直接指向 `ScalarInputRuntime`；也就是说，本轮只支持 **row-of-scalars**，不引入递归 row child；
- 若某些 merge fast path 需要 child flat raw 指针，应该由 `ScalarInputAdapter` 自身保证其 runtime payload 已经是可直接 load 的 canonical scalar 形态，而不是再给顶层 row runtime 增加“field0/field1 特例字段”。

### 10.5 Codegen 侧接口

codegen 层不再把“输入读取”硬编码成 `loadDecodedValue / loadDecodedRowField*` 这一组围绕 `HashAggrJitDecodedInput` 的 offset 访问，而是收敛成：

```cpp
class InputAdapterCodegen {
 public:
  virtual ~InputAdapterCodegen() = default;
  // 返回 IRRow<T> 中 payload 的 LLVM type。
  virtual llvm::Type* llvmValueType(HashAggrJitCodegen& codegen) const = 0;

  virtual llvm::Value* readIRRow(
      HashAggrJitCodegen& codegen,
      llvm::Value* runtime,
      llvm::Value* row,
      const HashAggrJitSlot& slot) const = 0;
};
```

第一阶段只需要两个实现：

- `ScalarInputAdapterCodegen`
  - 从 `ScalarInputRuntime` 读出 `IRRow<T>`
  - 覆盖今天 `DecodedScalarInputAdapter::readKnownNotNull/readNullable` 的职责
- `RowInputAdapterCodegen`
  - 从 `RowInputRuntime` 先读出 `children[i]` 对应的 `ScalarInputRuntime*`
  - 再通过内部持有的 scalar child readers 读取 child 的 value/null
  - 最后组装顶层 `IRRow<struct{...}>`
  - 覆盖今天 `loadDecodedRowField / loadDecodedRowFieldBool` 这组“按 field 特判”的路径

这样 IR 收敛点就从“旧 descriptor 的固定字段”变成“adapter runtime 的稳定形状”。

#### 10.5.0.1 `RowInputRuntime.children` 如何读到 child 的 values/nulls

这是 runtime / codegen 分层里最关键的一点：

- `RowInputRuntime` **不直接暴露** `child0Values/child0Nulls/...` 这类字段；
- `RowInputRuntime` 只保存 `children[i]` —— 即 `ScalarInputRuntime*`；
- “如何从这个 child runtime 读出 value/null” 由 `RowInputAdapterCodegen` 内部持有的 scalar child readers 决定。

推荐读取流程：

```cpp
auto* childRuntime = rowCodegen.loadScalarChildRuntime(codegen, rowRuntime, i);
auto* childRow = rowCodegen.scalarChildAt(i).readIRRow(codegen, childRuntime, row, slot);
auto* childValue = IRRow::getValue(builder, childRow);
auto* childIsNull = IRRow::getIsNull(builder, childRow);
```

也就是说：

1. row runtime 只负责提供 `children[i]` 指针；
2. 当前阶段 child 固定为 scalar，因此不需要 runtime tag，也不需要递归 row dispatch；
3. child 的 `values/nulls/indices` 在 `ScalarInputRuntime` 内部读取；
4. 因而“读 children 的 values/nulls”不是 `RowInputRuntime` 的接口，而是 `RowInputAdapterCodegen` 调度其 scalar child readers 的结果。

这也解释了为什么 row runtime 本身不需要再带：

- `indices`
- `rowField0Values`
- `rowField1Values`

因为这些都属于 child 的读取策略，不属于 row root 的职责。

#### 10.5.0.2 `InputAdapter` / `InputAdapterCodegen` 应如何调整

为了让上面的 child-reading 成立，接口要从“单节点一次性读完”调整成“runtime root + codegen 节点协作”：

##### 运行时对象层

`InputAdapter` 负责两件事：

1. 构造并拥有 runtime payload；
2. 暴露与之匹配的 codegen 节点。

也就是说，`InputAdapter::runtime()` 和 `InputAdapter::codegen()` 必须成对出现。

##### codegen 层

`InputAdapterCodegen` 基类仍然只需要：

1. `llvmValueType(...)`：告诉框架当前节点的 payload LLVM type；
2. `readIRRow(...)`：把当前 runtime 解释成 `IRRow<T>`。

row-specific 的 child 访问辅助接口不必上提到基类；它们由 `RowInputAdapterCodegen` 自身私有持有即可。这样接口更贴近当前“row-of-scalars”的范围，避免过早泛化。

##### `RowInputAdapterCodegen` 的实现语义

`RowInputAdapterCodegen::readIRRow(...)` 应按下面语义生成 IR：

1. 先检查 `rowRuntime.nulls`，得到 top-level row 是否为 null；
2. 若 top-level 为 null，返回 `IRRow<struct{...}>{zero_payload, true}`；
3. 若 top-level 非 null，则对每个 child：
   - `childRuntime = loadScalarChildRuntime(..., i)`
   - `childRow = scalarChildAt(i).readIRRow(..., childRuntime, row, slot)`
4. 组装 payload：
   - 若当前业务只需要 child value（例如 avg merge 的 `sum/count` 都是非 null 标量），可把 `IRRow::getValue(childRow)` 填入 payload；
   - 当前阶段只支持 scalar child；若未来要支持 nested row child，再重新把 row child codegen 抽象上提。

因此，**从 `children` 读 values/nulls 的正确模型，不是给 `RowInputRuntime` 增字段，而是让 `RowInputAdapterCodegen` 持有一组 scalar child readers，并逐个解释 `ScalarInputRuntime`。**

#### 10.5.1 `genAddDenseIR` 的 LLVM function 接口如何设计

结论先说：**可以去掉 `HashAggrJitDecodedInput`，而且 `genAddDenseIR` 的 LLVM ABI 不需要大改；最好的做法是“保留 3 参函数形状，只替换第 3 个参数的语义”**。

推荐接口：

```cpp
using HashAggrJitAddDenseFunc =
    void (*)(char** groups, int32_t numRows, char** inputRuntimes);
```

对应 LLVM：

```llvm
define void @jit_HashAggrAddDense(
    i8** %groups,
    i32 %num_rows,
    i8** %input_runtimes)
```

也就是说：

- 参数 1：`groups`，不变；
- 参数 2：`numRows`，不变；
- 参数 3：从今天的 `decodedInputs` 改成 **`inputRuntimes`**；
- `inputRuntimes[slotIndex]` 指向该 slot 对应 InputAdapter 持有的 root runtime payload；
- JIT 函数本身**不知道也不需要知道**这是 C++ 虚对象，只把它当成 adapter-owned POD runtime 根指针来读。

这样做的关键收益是：

1. `HashAggrJitChunk`、ORC JIT function pointer、调用侧大框架都几乎不用改 ABI；
2. `GroupingSet` 只需把 `hashAggrJitDecodedPtrs_` 的元素从“指向 `HashAggrJitDecodedInput`”改成“指向 adapter runtime”；
3. 热循环仍然是 `slotIndex -> load runtime ptr -> 直接 load values/indices/nulls`，不会引入 per-row virtual dispatch。

#### 10.5.2 为什么不建议把 LLVM 接口改成 `InputAdapter**`

不推荐这种形状：

```cpp
void (*)(char** groups, int32_t numRows, InputAdapter** adapters)
```

原因：

1. JIT 热路径若想通过 `InputAdapter**` 做 virtual call，会直接把虚调用引进每行循环；
2. LLVM 对 C++ vtable/object layout 没有必要也不应该感知；
3. 我们真正需要的是“稳定可 load 的 runtime payload”，而不是对象本身。

所以正确分层应该是：

- **C++ 对象层**：`InputAdapter` 虚接口，负责 batch 准备；
- **JIT ABI 层**：`i8** input_runtimes`，只传 POD payload 指针；
- **codegen 层**：由 slot 绑定的 adapter codegen helper 决定如何解释这个 payload。

#### 10.5.3 `genAddDenseIR` 内部如何按 slot 解释第 3 个参数

`genAddDenseIR(...)` 的 skeleton 推荐改成：

```cpp
for each slot i:
  runtime = load input_runtimes[i]
  if (checkInputNulls && !countStar) {
    if (slot.inputCodegen->topLevelIsNull(codegen, runtime, row)) {
      goto next_slot;
    }
  }
  addFn(codegen, group, runtime, row, slot, ...)
```

这里有两个重要点：

1. **slot 用哪个 adapter codegen，是编译期常量，不是运行期分派**；
2. 第 3 参始终只是 `i8* runtime`，真正如何解释成 scalar/row runtime，由该 slot 对应的 codegen helper 完成。

也就是说，`HashAggrJitOps::AddFn` 仍然可以保持“每个聚合一个 add 函数”的结构，但其参数语义应从：

```cpp
llvm::Value* decoded
```

改成：

```cpp
llvm::Value* inputRuntime
```

然后在 `sum/min/max/avg/...` 的 addFn 内部统一写成：

```cpp
auto* inputRow = slot.inputCodegen->readIRRow(codegen, inputRuntime, row, slot);
```

这样 GroupOps 看到的始终就是 `IRRow<T>`，不再碰 `HashAggrJitDecodedInput` 的字段偏移。

#### 10.5.4 运行时 payload 推荐形状

推荐 root runtime 只保留两种：

```cpp
struct ScalarInputRuntime {
  const void* values;
  const int32_t* indices;
  const uint64_t* nulls;
};

struct RowInputRuntime {
  const uint64_t* nulls;
  const ScalarInputRuntime* const* children;
};
```

这里刻意**不再放**：

- `decodedVector`
- `rowField0Values`
- `rowField1Values`

因为这些都是把框架重新绑回旧 descriptor / 特定聚合语义的回退路线。

avg merge / decimal sum merge 这类历史快路径，应该改为：

- `RowInputRuntime.children[0]` 指向 field0 的 `ScalarInputRuntime`
- `RowInputRuntime.children[1]` 指向 field1 的 `ScalarInputRuntime`

这样 JIT 仍然可以直接读 child flat raw values，并不会失去快路径。

#### 10.5.5 `genAddDenseIR` 的无 null 快路径怎么保留

这部分仍然建议保留今天的双函数模型：

- `addDense`：会做 top-level null check
- `addDenseNoNull`：不做 top-level null check

也就是 LLVM ABI 仍是同一个 3 参函数类型，只是生成两份实现。

变化点不在函数签名，而在 skeleton 里的 null 判断从：

```cpp
loadDecodedNulls(decoded)
```

变成：

```cpp
slot.inputCodegen->loadTopLevelNulls(runtime)
// or slot.inputCodegen->topLevelIsNull(...)
```

这样 scalar / row 输入都能复用同一套外层 skeleton，而不是把 null 逻辑重新散落到各个聚合实现里。

#### 10.5.6 对实现顺序的直接指导

因此真正落地时，`genAddDenseIR` 这条线建议按下面顺序改：

1. 先把 `decodedInputs` 变量/注释/语义重命名为 `inputRuntimes`；
2. 把 `HashAggrJitOps::AddFn` 的 `decoded` 参数语义改成 `inputRuntime`；
3. 在 slot 上挂 compile-time 的 input codegen/helper 信息；
4. 把外层 null gating 改成走 adapter helper；
5. 最后再删 `HashAggrJitDecodedInput`、`offsetof(...)` 常量与 `loadDecoded*` 专名 API。

**所以答案是：能去掉，而且最合理的 `genAddDenseIR` 设计不是改成“传 adapter 对象”，而是保留 `void(i8**, i32, i8**)` 形状，把第三个参数升级成 adapter-owned runtime payload 数组。**

#### 10.5.7 `HashAggrJitDecodedInput` 是否应该改成 union

这个方向**是可行的，而且比“继续扩一个大 struct”更优**；在当前 bolt hash aggr JIT 这条路径里，
我现在进一步收敛为：

- **顶层输入 runtime 可以直接用无 tag 的 union root**
- scalar / row 由 **codegen 时已知的 adapter 结构** 决定，而不是由 runtime node 自描述
- 但 **row 的 child 当前进一步收紧为 scalar-only**，不再让 child 也走统一 union node

推荐形状：

```cpp
union HashAggrJitInputRuntime;

struct HashAggrJitScalarInputRuntime {
  const void* values;
  const int32_t* indices;
  const uint64_t* nulls;
};

struct HashAggrJitRowInputRuntime {
  const uint64_t* nulls;
  const HashAggrJitScalarInputRuntime* const* children;
  int32_t numChildren;
};

union HashAggrJitInputRuntime {
  HashAggrJitScalarInputRuntime scalar;
  HashAggrJitRowInputRuntime row;
};
```

##### 为什么 union 方向是对的

因为它解决了当前 `HashAggrJitDecodedInput` 最大的问题：

1. **把 scalar / row 两类输入形状显式分开**，而不是塞进一个横向扩字段的大 struct；
2. `rowField0Values / rowField1Values` 这种“为了某个聚合临时开洞”的模式可以消失；
3. 第三个参数仍然可以是“runtime root 指针数组”，不影响 `genAddDenseIR` 的 3 参 ABI 形状；
4. InputAdapter 的职责能真正落到“从 union runtime 读出 `IRRow`”，而不是继续围绕旧 `DecodedInput` 的字段偏移打补丁。

##### 为什么这里可以省掉 shape/tag

因为当前 add_dense 的生成方式决定了：

1. 每个 slot 在 codegen 时已经知道输入是 scalar 还是 row；
2. 当前 row 的每个 child 固定为 scalar，child 的读取方式在 codegen 时也是已知的；
3. 热路径不需要 runtime shape dispatch，只需要按已知形状直接 load 对应字段。

因此，对 bolt 这条 JIT 路线而言，runtime node 的职责就是“承载值指针/nulls/children（以及 scalar 自己的 indices）”，
而不是“再告诉 JIT 自己是什么类型”。

##### 无 tag union 的前提条件

无 tag union 成立的前提是：

1. **不能在热路径做 runtime kind 分派**；
2. slot 必须绑定 compile-time 的 input codegen/helper；
3. row child 的访问路径必须来自已知的 adapter 结构，而不是依赖 runtime 自描述；
4. 若需要 debug/assert，应由构造阶段或非 hot 校验逻辑承担，而不是把 tag 常驻在 runtime node 上。

也就是说：**union 是 runtime 承载方式，shape 是 codegen 元信息，不必塞进 runtime node。**

##### 与 `genAddDenseIR` 接口的关系

即便采用无 tag union，`genAddDenseIR` 的 LLVM 接口也**不需要**变成复杂签名；仍建议保持：

```cpp
void (*)(char** groups, int32_t numRows, char** inputRuntimes)
```

或者在 C++ typedef 层写成更强语义版本：

```cpp
using HashAggrJitAddDenseFunc =
    void (*)(char** groups,
             int32_t numRows,
             HashAggrJitInputRuntime* const* inputRuntimes);
```

但 LLVM IR 里依然可以保持 `i8**`，避免 ABI 扩散。

##### 什么时候 union 比“分离 root struct + void*”更优

我现在更偏向这个简化后的 union 方案，前提是满足下面两点：

1. 顶层输入 runtime 统一收敛到 scalar/row 两种 root 形状；
2. RowInputRuntime 的 child 固定为 scalar runtime，由 `RowInputAdapterCodegen` 的 scalar child readers 解释成 `IRRow`。

因为当前实际需求只覆盖 row-of-scalars，这比“外面全是 `void*`，每层都靠约定 cast”更稳，也比提前支持递归 row 更容易落地。

##### 什么时候 union 仍然不够好

如果只是把当前这个 struct 生硬改成：

- 一个 scalar variant
- 一个仍然带 `rowField0/rowField1` 的 row variant

那仍然不够好，因为这只是把“avg / decimal sum 的聚合语义”从 struct 平铺变成 union variant，**没有真正建立 generic row runtime**。

所以采用这一版 union 的最低要求是：

- row variant 只能有 `nulls/children/numChildren`
- `children` 必须直接指向 `ScalarInputRuntime`
- 不能再出现 `field0/field1` 这种聚合专有字段名

##### 结论

因此，对“是否可将 `HashAggrJitDecodedInput` 改成 union，并将 union 指针作为 add_dense 第三个参数传入 LLVM function”这个问题，我的结论是：

- **可以，而且方向是对的；**
- **比继续沿用一个大而全的 struct 更优；**
- **在当前 bolt JIT 路线里，runtime node 可以不带 shape/tag；**
- **并且当前阶段 row variant 应进一步限定为 scalar-children 形状，否则实现复杂度会明显超前于需求。**

### 10.6 为什么不会导致性能回退

性能保护原则：

1. **不在每行调用虚函数**：virtual dispatch 仅用于 batch 准备；
2. **不在每行调用通用 runtime helper**：常见标量输入仍展开成直接 load `values + indices[row]` / `nulls[row]`；
3. **保留 raw child fast path**：row merge 若 child 已是 flat canonical scalar runtime，codegen 直接读取 child runtime，不退回 `DecodedVector` helper；
4. **让 encoding 差异前置到 adapter 构造**：dictionary/constant/flat 的分歧在 adapter `prepare()` 内吸收，JIT IR 只面对 canonical runtime payload；
5. **外层 add-dense skeleton 不被打散**：只替换“单 slot 如何读 input”，不引入额外 per-row 框架判断。

换句话说，InputAdapter 的虚接口是**对象建模边界**，不是热循环执行模型。

### 10.7 与当前 patch 序列的衔接

当前已经落下的 `IRRow`、`DecodedScalarInputAdapter`、`FlatOutputAdapter` 可以视为最终架构的前置垫片：

- `IRRow`：保留，作为三层之间唯一值契约；
- `DecodedScalarInputAdapter`：后续升格为 `ScalarInputAdapterCodegen`，不再依附旧 `HashAggrJitDecodedInput` 命名；
- `RowOutputAdapter`：必须保持 generic，只按 struct field 写回，不认 avg 的 2-field 语义；
- `HashAggrJitCodegen::loadDecoded*`：逐步收缩为 adapter runtime 读取 helper，最终删掉 decoded-input 专名 API。

### 10.8 建议迁移顺序

#### Phase A：先把 codegen 边界改对

1. 把当前 `RowOutputAdapter` 改成真正 generic 的 struct writer；
2. 在 `HashAggrJit.h/.cpp` 中引入 input runtime union / adapter codegen 概念；
3. 让 avg partial、decimal merge 等 row 输入/输出先走 generic row contract，而不是 field0/field1 语义 helper。

#### Phase B：引入 runtime InputAdapter 对象，但暂不改 add_dense ABI

1. `GroupingSet` 内部改为构造 `ScalarInputAdapter` / `RowInputAdapter`；
2. adapter 自己持有 runtime payload；
3. 传给 JIT 的仍可先保持 `char** inputs`，但每个元素改为指向 adapter-owned runtime，而不是 `HashAggrJitDecodedInput`。

这一阶段完成后，`HashAggrJitDecodedInput` 已经可以从执行路径移除，只剩个别 helper / test 兼容点。

#### Phase C：删除旧 descriptor 与旧命名 helper

1. 删除 `HashAggrJitDecodedInput`；
2. 删除 `loadDecodedValue/loadDecodedNulls/loadDecodedRowField*` 这组旧 API；
3. 测试与 benchmark 一律改用 adapter 构造路径；
4. 清理 `offsetof(HashAggrJitDecodedInput, ...)` 常量与相关 runtime helper。

### 10.9 本章对应的 DoD 补充

完成 InputAdapter 重构后，应额外满足：

- [ ] `GroupingSet` 不再直接构造 `HashAggrJitDecodedInput`；
- [ ] JIT add-dense ABI 传递的是 adapter-owned runtime payload；
- [ ] row merge / row extract 不再出现 `rowField0/rowField1` 这类聚合专有字段名；
- [ ] 新增一个 3-field intermediate 聚合时，不需要修改 InputAdapter/OutputAdapter 基类接口。

---

## 11. 事故复盘：`munmap_chunk(): invalid pointer`（commit `0722a59851` 引入）

本章记录一次在 `HashAggrJitBenchmark` 上复现的堆破坏崩溃的完整定位过程与根因，作为 output runtime 绑定相关改动的回归警示。

### 11.1 现象

- 运行 `bolt_hashaggr_jit_benchmark`（RelWithDebInfo 行为，Release preset 构建）必崩。
- 报错：`munmap_chunk(): invalid pointer`，`SIGABRT`。
- 栈顶在算子关闭阶段析构中间结果 vector 时：
  - `Driver::closeOperators()` → 释放 `RowVector` → 释放其 child `FlatVector<long>`(int64) 的 values buffer → glibc `free` 检测到非法 chunk 指针。
- hint：bug 出现在最近 5 个 commit 中。

### 11.2 定位过程

1. **缩小到具体 case**：在 benchmark `addCase()` 的 warmup 处加临时 `fprintf` 打印每个 case 名（每个 case warmup 时会先跑 nojit 再跑 jit）。运行后最后一条输出停在 `width4_merge_decimal_avg` 的 `jit` 阶段，**坐实崩溃 case = `width4_merge_decimal_avg`**。
2. **bisect 到 commit**：先前已通过 `git reset --hard` 确认 first bad commit = `0722a59851`（其 parent `f752929ecc` 不崩）。
3. **gdb 观察**：
   - 崩溃发生在第二阶段（final aggregation）输出路径 `GroupingSet::runHashAggrJitExtractChunks`。
   - 在 decimal avg 的两个 helper 上下断：`jit_HashAggrExtractPartialDecimalAvg` 被调用 40000 次，但 `jit_HashAggrExtractFinalDecimalAvg` **一次都没进入**就崩了 → 说明堆已在 final 阶段“**extract 绑定阶段**”（`chunk.extract()` 之前）被破坏。
4. **类型/精度推演**：
   - `width4` 用 `DECIMAL(12,2)`（short decimal）。
   - decimal avg 中间 sum 类型按签名 `ROW(DECIMAL(38, a_scale), BIGINT)` → `DECIMAL(38,2)` 是 **long decimal（int128）**。
   - decimal avg final 结果类型 `r_precision=min(38,12+4)=16` → `DECIMAL(16,6)` 是 **short decimal**，存储为 **`FlatVector<int64_t>`**。
   - 但 descriptor 的 `accumulatorKind = Int128`（见 `AverageAggregate.cpp` 的 `DecimalAverageAggregate::createHashAggrJitDescriptor`）。

### 11.3 根因

`0722a59851` 把 `GroupingSet.cpp` 里的 `hashAggrJitRawOutputValues`（改名为 `hashAggrJitRawOutputData`）的 `Int128` 分支，从父 commit 的 `return nullptr` 改成了：

```cpp
case jit::HashAggrJitValueKind::Int128:
  return vector->asUnchecked<FlatVector<int128_t>>()->mutableRawValues();
```

而 `runHashAggrJitExtractChunks` 的 **scalar final 输出绑定**（`GroupingSet.cpp:1306` 附近）用 `slot.desc.accumulatorKind` 来解释输出列：

```cpp
.values = hashAggrJitRawOutputData(aggregateVector.get(), slot.desc.accumulatorKind)
```

对 decimal avg final：`accumulatorKind == Int128`，但 final 输出列真实类型是 short-decimal `FlatVector<int64_t>`。于是：

1. 一个真实 `FlatVector<int64_t>` 被 `asUnchecked<FlatVector<int128_t>>()` 强转（类型混淆）。
2. 调用 `mutableRawValues()`（见 `FlatVector.h:244`）：此时 `values_` 是按 int64（8B/elem）分配且非 mutable，函数进入重分配分支：
   - 按 `int128`（16B/elem）**重新分配 buffer**；
   - `memcpy(newValues, rawValues_, byteSize<int128_t>(length))` 即按 2× 字节数从只有 8B/elem 的旧 buffer **越界读**；
   - 把该 vector 的 `values_` / `rawValues_` 替换成 int128 尺寸 buffer。
3. 这步破坏堆（越界读踩坏相邻 chunk metadata，并把列状态搞乱），最终在算子析构释放该 `RowVector`/`FlatVector` 链时 glibc 报 `munmap_chunk(): invalid pointer`。

**为何 parent commit 不崩**：原 `Int128` 分支 `return nullptr`，从不触碰该列 buffer。decimal avg final 真正写入走 helper `jit_HashAggrExtractFinalDecimalAvg`，由 `longDecimal` flag 正确按 int64/int128 写回，**根本不需要这个预取的 raw values 指针**。

**关键定性**：crash 由 commit `0722a59851` 的这一行引入（`bolt/exec/GroupingSet.cpp` 内 `hashAggrJitRawOutputData` 的 `Int128` 分支），与 scalar-output 绑定处用 `accumulatorKind` 解释 short-decimal 输出列的错配共同作用。它本质是一个 **`accumulatorKind` ≠ 输出 vector 实际存储类型** 的类型混淆。

### 11.4 验证

把 `Int128` 分支临时改回 `return nullptr`（仅验证用，注释说明 Int128 scalar/decimal 输出走 helper 的 `vector()`，不读此 raw 指针），重编译运行：

- `width4/8/16/32_merge_decimal_avg` 全部通过，crash 消失；
- 整个 benchmark 跑完无 `munmap` / `Aborted`。

→ 根因实锤。

### 11.5 修复（已实施：方案 1）

采用 §11.5 的方案 1：**scalar output 绑定按输出 vector 真实类型推导 kind**，而非 `accumulatorKind`。

`runHashAggrJitExtractChunks` 的 FLAT scalar 输出绑定改为：

```cpp
const auto outputKind = hashAggrJitOutputValueKind(aggregateVector.get());
if (!outputKind.has_value()) {
  canRunChunk = false;
  skipReason = "unsupported scalar output value kind";
  break;
}
... .values = hashAggrJitRawOutputData(aggregateVector.get(), *outputKind) ...
```

`hashAggrJitOutputValueKind` 已存在，会按列真实类型（含 short/long decimal）推导 kind，从而保证 `hashAggrJitRawOutputData` 取到的指针宽度与列存储宽度一致，杜绝 int64↔int128 错配重分配。`hashAggrJitRawOutputData` 的 `Int128` 分支保持正常实现（用于真正的 long-decimal/HUGEINT 输出列）。

其余备选方向（方案 2/3）未采用，记录备查：

1. 对走 helper 的 decimal/Int128 输出不预取 raw values（保持 nullptr）；
2. 统一约束指针宽度一致。

### 11.6 临时改动清理（已完成）

- `bolt/exec/benchmarks/HashAggrJitBenchmark.cpp`：`addCase()` 内的 `fprintf` case 名打印 —— **已回退**。
- `bolt/exec/GroupingSet.cpp`：`hashAggrJitRawOutputData` 的 `Int128` 分支临时 `return nullptr` —— **已恢复**为正常实现；正式修复落在 scalar 绑定处（见 §11.5）。

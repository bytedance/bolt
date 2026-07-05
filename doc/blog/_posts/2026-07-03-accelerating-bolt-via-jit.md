---
layout: post
title: "Accelerating Bolt via JIT"
date: 2026-07-03
author: "Xianda Ke"
parent: Blog
nav_order: 13
---

## Motivation for JIT

In a compute engine, operator implementations must be inherently generic. They need to handle a wide variety of scenarios, as user tables possess different schemas and query plans can vary significantly.

In a generic operator implementation, general-purpose C++ code inevitably requires various forms of runtime dispatching. This is the cost of abstracting operators. Such abstractions introduce runtime overhead and negatively impact code size, CPU cache hit rates, and branch prediction. While C++ templates can optimize some of these cases, they are difficult to apply to components that change dynamically at runtime. To address this, Bolt introduces a Just-In-Time (JIT) compiler for row-level optimization.

The introduction of JIT provides two major benefits:

1. **Performance Improvements:** The generated code executes fewer instructions, suffers fewer branch prediction misses, and achieves a significantly better CPU cache hit rate.
2. **Hardware-Specific Optimization:** Bolt detects the CPU architecture at runtime and generates code optimized for specific instruction sets, including x86_64 AVX-512 and ARM SVE.

Unlike some systems, Bolt does not generate C/C++ source code to be compiled by an external compiler. Instead, it directly generates LLVM Intermediate Representation (IR). In our measurements, invoking a C++ compiler can take more than seven seconds, which is unacceptable for ad-hoc query scenarios.

## Design Requirements for the Bolt JIT Engine

Our design fulfills the following key requirements:

- **Fine-grained lock control and concurrent compilation:** This keeps compilation latency at the sub-second level, which is crucial for ad-hoc and high-QPS (Queries Per Second) scenarios. At the IR compilation entry point (`ThrustJITv2::CompileModule`), there is no global lock. The most expensive compilation optimization and linking steps run concurrently across multiple threads, ensuring multiple compilation requests do not queue behind a single bottleneck.
- **Memory resource usage accounting and control:** Bolt customizes the `ObjectLinkingLayer` to track the size of generated code and use it for precise memory resource control.
- **Code caching:** To reduce compilation costs, compiled code is managed by a `CompiledModule` handler. When business logic releases it, the generated code is not immediately discarded; instead, it is placed in an `LRUCache` for future reuse.

We rebuilt our previous implementation based on LLVM 19 ORCv2 and named it `ThrustJITv2`. A significant benefit of using a newer LLVM version is its ability to produce better-optimized machine code for modern hardware platforms.

> **Note:** This document does not cover the specifics of LLVM ORCv2. For more information, please refer to the [LLVM IR](https://llvm.org/docs/LangRef.html) and [ORCv2](https://llvm.org/docs/ORCv2.html) documentation.

## Using Bolt JIT

To demonstrate the basic workflow of using Bolt JIT, consider this trivial example that generates an `int64_t sum(int64_t, int64_t)` function:

```cpp
using SumFunc = int64_t (*)(int64_t, int64_t);
const std::string funcName = "sum";

auto generateSumIR = [funcName](llvm::Module& llvmModule) -> bool {
    auto& llvmContext = llvmModule.getContext();
    llvm::IRBuilder<> builder(llvmContext);
    auto* int64Ty = builder.getInt64Ty();
    auto* funcType = llvm::FunctionType::get(int64Ty, {int64Ty, int64Ty}, false);
    auto* func = llvm::Function::Create(funcType, llvm::Function::ExternalLinkage, funcName, llvmModule);
    auto* args = func->args().begin();
    args->setName("a");
    (++args)->setName("b");
    auto* entry = llvm::BasicBlock::Create(llvmContext, "entry", func);
    builder.SetInsertPoint(entry);
    llvm::SmallVector<llvm::Value*, 2> values;
    for (auto& arg : func->args()) {
      values.push_back(&arg);
    }
    builder.CreateRet(builder.CreateAdd(values[0], values[1], "addtmp"));
    return llvm::verifyFunction(*func, &llvm::errs());
};

auto* thrustJIT = ThrustJITv2::getInstance();
auto compiledModule = thrustJIT->CompileModule(generateSumIR, funcName);

// In your business logic, keep the compiledModule object alive. This guarantees
// the lifetime of the JIT code. After compiledModule is released, the JIT code
// is unloaded.
auto sumFunc = reinterpret_cast<SumFunc>(compiledModule->getFuncPtr(funcName));
EXPECT_EQ(sumFunc(100, 200), 300);
```

While this example is trivial, it illustrates how LLVM IR is generated and passed to `ThrustJITv2`. Next, let's explore how the Bolt compute engine leverages JIT in practice.

## Operator Optimization via JIT

`RowContainer` is an important low-level data structure in Bolt. It is used in critical operators such as Sort, Aggregate, and Join, making its performance vital to the overall engine.

This section uses the Sort operator as an example to explain how Bolt uses JIT to optimize operator implementations.

Consider the SQL statement:
`SELECT name, age, info FROM tmp ORDER BY name DESC, age`

During sorting, data comparison is a core operation. The generic C++ implementation looks like this:

```cpp
int RowContainer::compare(
    const char* FOLLY_NONNULL left,
    const char* FOLLY_NONNULL right,
    int columnIndex,
    CompareFlags flags) {
  auto type = types_[columnIndex].get();
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compare, type->kind(), left, right, type, columnAt(columnIndex), flags);
}
```

This generic implementation introduces the following runtime overhead:

- **Type dispatching:** Introduced by the `BOLT_DYNAMIC_TYPE_DISPATCH_ALL` macro.
- **Position recalculation:** Recomputing the column offset for every row (e.g., `columnAt(columnIndex)`).
- **Branching overhead:** Evaluating `flags` at runtime for conditions like ascending or descending order.

In Bolt, `RowContainer` passes its metadata (e.g., data types, nulls first, ascending/descending order) to `RowContainerCodeGenerator`. Based on this context, `RowContainerCodeGenerator` generates the corresponding LLVM IR at runtime. This IR is then passed to the JIT engine (`ThrustJITv2`) to generate optimized machine code.

Here is the implementation:

```cpp
RowContainerCodeGenerator codeGenerator;
codeGenerator.setKeyTypes(std::move(keyTypesCopy))
        .setCompareFlags(std::move(flags))
        .setKeyOffsets(std::move(keyOffsets))
        .setNullByteOffsets(std::move(nullByteOffsets))
        .setNullMasks(std::move(nullByteMasks))
        .setHasNullKeys(hasNullKeys)
        .setStringViewsAreContiguous(useMonotonicStringAllocation_)
        .setOpType(cmpType);

auto fnName = codeGenerator.GetCmpFuncName();
jitModule_ = codeGenerator.codegen();

// ...
jitted_cmp_func = (RowRowCompare)jitModule_->getFuncPtr(rowRowCmpfn);
sorter_.sort(sortedRows_.begin(), sortedRows_.end(), jitted_cmp_func);
```

The generated LLVM IR is shown below (truncated for brevity):

```llvm
define i8 @jit_rr_sort_lessN7_LD3_LA016(ptr %l, ptr %r) {
entry:
  br label %key_0
key_0:                                            ; preds = %entry
  %0 = getelementptr inbounds i8, ptr %l, i64 20
  %1 = getelementptr inbounds i8, ptr %r, i64 20
  %2 = load i8, ptr %0, align 1
  %3 = load i8, ptr %1, align 1
  %4 = and i8 %2, 1
  %5 = and i8 %3, 1
  %6 = icmp ne i8 %4, %5
  br i1 %6, label %key_0_nil_ne_blk, label %key_0_nil_eq_blk, !prof !0
  ; ... ...
key_0_no_nil_blk:                                 ; preds = %key_0_nil_eq_blk
  %10 = getelementptr inbounds i8, ptr %l, i64 0
  ; ... ...
```

This generated IR offers several performance improvements over the generic C++ implementation:

- **Constant offsets:** The position calculation for each value in a row uses direct constants. For example, `%0 = getelementptr inbounds i8, ptr %l, i64 20` shows that the offset is calculated directly with the constant `20`.
- **Eliminated type dispatching:** Type-checking logic is entirely removed.
- **Inlined comparisons:** Function calls for comparing primitive types are eliminated.
- **Simplified branching:** `if` branch checks for runtime flags (e.g., `nulls_first`, descending order) are resolved at compile time.

## Benchmarks

We benchmarked the Sort operator on a development machine, yielding the following results:

| `orderBy` Keys | JIT | Non-JIT |
|---|---:|---:|
| 3 `i64` keys | 3.55 s | 8.06 s |
| 3 short string keys | 27.1 s | 64.7 s |
| 3 long string keys | 42.2 s | 68.0 s |

Bolt JIT has been validated at scale in online production workloads. Feedback from a production preprocessing workload indicates that enabling Bolt JIT yields a 10% overall performance improvement.

## Future Work

### JIT Engine Optimization

- **Asynchronous compilation:** Currently, execution blocks until the JIT function is generated. In the future, we plan to adopt a lazy, asynchronous approach: falling back to the generic C++ logic while the JIT function is compiling in the background, and seamlessly switching to the JIT-compiled function once it is ready.

### Expanded JIT Coverage

- **Aggregate functions codegen:** Currently under development.
- **Whole-stage codegen:** Exploring the feasibility of generating code for entire query stages.

---
layout: post
title: "Refactoring HiveHash for Faster Shuffle Hashing"
date: 2026-07-03
author: "Zhang Xiaofeng"
parent: Blog
nav_order: 11
---

HiveHash is used by Bolt's Spark SQL hash functions to produce Hive-compatible
hash values for primitive and complex types. In aggregation workloads, this
hash computation sits on the shuffle path, so small per-row overhead can become
very expensive at scale. This post walks through a refactor of
[`Hash.cpp`](https://github.com/bytedance/bolt/blob/main/bolt/functions/sparksql/Hash.cpp)
that removes hot-loop type work, enables SIMD-friendly paths, and changes the
hashing order for nested types.

## Motivation

When investigating Spark jobs with aggregations, we noticed that shuffle hash
computation was very time-consuming: 65.71 hours in one job and 154.61 hours in
another. It even cost more time than shuffle split, which took 27.03 hours and
133.16 hours. That was extremely abnormal.

```text
Before the refactor:

Project 1:
  shuffle hash compute: 65.71 h
  shuffle split:        27.03 h

Project 2:
  shuffle hash compute: 154.61 h
  shuffle split:        133.16 h
```

The key observation was:

- The first project's hash input types were three `BIGINT` columns.
- The second project's hash input types were three `BIGINT` columns and one
  string column.

This suggested that Bolt might have performance issues when hashing `BIGINT`
and string values.

## Benchmark Test

We then ran benchmark tests. The result showed that, compared with hashing
`i32`, hashing `i64` cost about 4x more.

```text
Initial HiveHash benchmark:

Case                         time/iter   iters/s
HiveHash##i32                105.67 ms   9.46
HiveHash##i64                436.43 ms   2.29
HiveHash##f32                113.75 ms   8.79
HiveHash##f64                480.54 ms   2.08
HiveHash##d64                401.60 ms   2.49
HiveHash##d128              772.65 ms   1.29
HiveHash##str                569.61 ms   1.76
HiveHash##row_i32_i32        351.02 ms   2.85
HiveHash##row_i64_i64          1.00 s    999.09 m
HiveHash##row_str_str          1.25 s    801.52 m
HiveHash##array_i32          501.93 ms   1.99
HiveHash##array_i64            3.48 s    287.68 m
HiveHash##array_str            4.67 s    214.22 m
HiveHash##map_i32_i32          1.51 s    662.68 m
HiveHash##map_i64_i64          7.58 s    131.87 m
HiveHash##map_str_str         10.72 s    93.32 m
```

## Bigint, Decimal64, and Double

The HiveHash `i64` implementation was:

```cpp
ReturnType hashInt64(int64_t input, SeedType seed, const TypePtr& inputType) {
  if (const auto& shortDecimal =
          std::dynamic_pointer_cast<const ShortDecimalType>(inputType)) {
    if (input == 0) {
      return genSeed(seed);
    }
    uint8_t newScale = normalizeDecimal(input, shortDecimal->scale());
    return genSeed(seed) + decimalHashCode(input, newScale);
  } else {
    return genSeed(seed) +
        (input ^ static_cast<uint32_t>(static_cast<uint64_t>(input) >> 32));
  }
}
```

The important detail is the `std::dynamic_pointer_cast` inside the hash path.
For non-decimal `BIGINT`, this type check still happened during hash
computation.

The performance issue in `hive_hash` for `BIGINT` came from `dynamic_cast`
during hash computation.

If we write the original hasher logic as pseudocode, it looks like this:

```cpp
Hasher hasher;
switch (type->kind()) {
  case INTEGER:
    rows.applyAllSelected([&](vector_size_t row) {
      resultValues[row] = hasher.hashInt32(row);
    });
    break;
  case BIGINT:
    rows.applyAllSelected([&](vector_size_t row) {
      resultValues[row] = hasher.hashInt64(row);
    });
    break;
  // ...
}
```

To avoid `dynamic_cast` in each hash computation, we let `Hasher` hold the type
information:

```cpp
switch (type->kind()) {
  case INTEGER: {
    Hasher<TypeKind::INTEGER> hasher(type);
    rows.applyAllSelected([&](vector_size_t row) {
      resultValues[row] = hasher.hash(row);
    });
    break;
  }
  case BIGINT: {
    Hasher<TypeKind::BIGINT> hasher(type);
    rows.applyAllSelected([&](vector_size_t row) {
      resultValues[row] = hasher.hash(row);
    });
    break;
  }
  // ...
}
```

After this change, the numeric benchmark looked like this:

```text
After avoiding dynamic_cast in the hot path:

Case            time/iter   iters/s
HiveHash##i32   63.35 ms    15.78
HiveHash##i64   57.03 ms    17.53
HiveHash##f32   68.60 ms    14.58
HiveHash##f64   65.73 ms    15.21
```

`HiveHash##i64` and `HiveHash##f64` became much faster, moving from more than
400 ms to about 60 ms.

If we use `perf` on `HiveHash##i64`, we can see that hashing is done with
simple shift and subtract instructions, without SIMD instructions.

```text
perf annotate after dynamic-cast removal:

Samples: 5K of event 'cpu-clock:uhpppH', 4000 Hz
Event count (approx.): 1436250000
Symbol:
  facebook::velox::functions::sparksql::(anonymous namespace)::
  HashFunctionEvaluator<...>

Key sampled instructions:
  8.74%  push   %r12
  9.15%  push   %rbx
  8.05%  mov    %r12,%rdi
          callq  facebook::velox::DecodedVector::isNullAt
 10.13%  lea    0x0(,%rbx,4),%rsi
  9.44%  add    %rsi,%rdi
  8.25%  sub    %eax,%ecx

Observation:
  The hot code is scalar shift/sub/add style code, not SIMD.
```

The benchmark input had no nulls. If we special-case no-null flat vectors, the
compiler may generate SIMD instructions for hash computation:

```cpp
HiveHash<typeKind> hasher(input->type());
if (input->isFlatEncoding()) {
  auto flatVector =
      input->asFlatVector<typename TypeTraits<typeKind>::NativeType>();
  bool hasNoNull = flatVector->rawNulls()
      ? bits::isAllSet(flatVector->rawNulls(), 0, inputSize, bits::kNotNull)
      : true;
  if (hasNoNull) {
    auto rawInput = flatVector->rawValues();
    for (size_t row = 0; row < inputSize; row++) {
      result[row] = hasher.hash(rawInput[row], result[row]);
    }
  } else {
    for (size_t row = 0; row < inputSize; row++) {
      HiveHashBase::ResultType hashValue;
      if (flatVector->isNullAt(row)) {
        hashValue = HiveHashBase::hashNull(result[row]);
      } else {
        hashValue = hasher.hash(flatVector->valueAtFast(row), result[row]);
      }
      result[row] = hashValue;
    }
  }
}
```

After this change, the benchmark result became:

```text
After the no-null flat-vector path:

Case            time/iter   iters/s
HiveHash##i32   16.78 ms    59.60
HiveHash##i64   20.26 ms    49.36
HiveHash##f32   22.41 ms    44.62
HiveHash##f64   21.43 ms    46.67
```

Case time went from more than 60 ms to about 20 ms. Looking at the perf result,
the HiveHash computation was generated with good SIMD instructions.

```text
perf top after the no-null flat-vector path:

Samples: 5K of event 'cpu-clock:uhpppH'
Event count (approx.): 1412250000

Overhead  Command         Shared Object                   Symbol
43.16%    velox_sparksql  velox_sparksql_benchmarks_hash  HashFunctionEvaluator<...>
24.85%    velox_sparksql  velox_sparksql_benchmarks_hash  std::_Function_handler<...>
15.52%    velox_sparksql  velox_sparksql_benchmarks_hash  hiveHashMultiple<TypeKind::INTEGER>
 1.17%    velox_sparksql  velox_sparksql_benchmarks_hash  exec::Expr::eval
```

```text
perf annotate for hiveHashMultiple<TypeKind::INTEGER>:

Samples: 5K of event 'cpu-clock:uhpppH', 4000 Hz
Event count (approx.): 1412250000

Key SIMD instructions:
  0.11%  vmovdqu      0x20(%r11,%rax,2),%xmm6
         vinserti128  $0x1,0x10(%r11,%rax,2),%ymm5,%ymm1
  9.69%  vinserti128  $0x1,0x30(%r11,%rax,2),%ymm6,%ymm3
  4.10%  vpsrlq       $0x20,%ymm1,%ymm2
  6.39%  vpsrlq       $0x20,%ymm3,%ymm4
  2.39%  vpshufd      $0xd8,%ymm0,%ymm0
  7.41%  vpshufd      $0xd8,%ymm2,%ymm2
  1.25%  vperm2i128   $0x20,%ymm3,%ymm1,%ymm0
  5.02%  vperm2i128   $0x31,%ymm3,%ymm1,%ymm1
  6.50%  vpxor        %ymm2,%ymm0,%ymm0
 25.66%  vpslld       $0x5,%ymm1,%ymm2
         vpsubd       %ymm1,%ymm2,%ymm1
         vpaddd       %ymm1,%ymm0,%ymm0

Observation:
  The hash loop now contains vector instructions.
```

However, from the perf result above, the top time-consuming function became
`HashFunctionEvaluator::apply`. Looking at the detailed instructions:

```text
perf annotate for HashFunctionEvaluator::apply:

Samples: 5K of event 'cpu-clock:uhpppH', 4000 Hz
Event count (approx.): 1412250000

Key sampled instructions:
        callq  SelectivityVector::isAllSelected
  2.30% andl   $0x7fffffff,(%rdx,%rax,4)
 55.00% add    $0x1,%rax
        cmp    %eax,0x20(%r15)
        jg     ...

Observation:
  Clearing the signed bit was still executed in a scalar row loop.
```

We can locate the corresponding C++ code. After the HiveHash result is
returned, Bolt clears the signed bit of all results.

```cpp
static void apply(
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    std::optional<typename HashClassBase::SeedType> seed,
    exec::EvalCtx& context,
    VectorPtr& resultRef) {
  size_t hashIdx = seed ? 1 : 0;
  auto hashSeed = seed ? *seed : HashClassBase::kDefaultSeed;

  auto& result = *resultRef->as<FlatVector<ResultType>>();
  auto rawValues = result.mutableRawValues();
  result.clearNulls(rows);
  rows.applyToSelected([&](int row) { rawValues[row] = hashSeed; });

  for (auto i = hashIdx; i < args.size(); i++) {
    SWITCH_TYPE_HASH(args[i]->type(), hashOneColumn, rows, args[i], result);
  }
  if constexpr (std::is_same_v<HashClassBase, HiveHashBase>) {
    rows.applyToSelected([&](int row) { rawValues[row] &= 0x7FFFFFFF; });
  }
}
```

The hot line is the final mask applied to every HiveHash result:

```cpp
rows.applyToSelected([&](int row) { rawValues[row] &= 0x7FFFFFFF; });
```

Why was this not done with SIMD instructions? Inspecting `applyToSelected`, we
found that it has a separate path for all selected rows. The loop end used a
member variable, `end_`, so the compiler had to call `func` one by one in case
`end_` was reassigned.

Before the fix:

```cpp
template <typename Callable>
inline void SelectivityVector::applyToSelected(Callable func) const {
  if (isAllSelected()) {
    for (vector_size_t row = begin_; row < end_; ++row) {
      func(row);
    }
  } else {
    bits::forEachSetBit(bits_.data(), begin_, end_, func);
  }
}
```

The loop condition reads `end_` directly.

To fix this, we simply assigned `end_` to a local `const` variable:

```cpp
template <typename Callable>
inline void SelectivityVector::applyToSelected(Callable func) const {
  if (isAllSelected()) {
    // Use a const variable to help the compiler generate more effective code.
    const vector_size_t end = end_;
    for (vector_size_t row = begin_; row < end; ++row) {
      func(row);
    }
  } else {
    bits::forEachSetBit(bits_.data(), begin_, end_, func);
  }
}
```

The key change is caching `end_` in a local constant:

```cpp
const vector_size_t end = end_;
```

After this change, the benchmark result became:

```text
After caching applyToSelected end_ in a local constant:

Case            time/iter   iters/s
HiveHash##i32    7.22 ms    138.49
HiveHash##i64   10.19 ms     98.10
HiveHash##f32   10.77 ms     92.83
HiveHash##f64   11.79 ms     84.80
```

The corresponding instructions are shown below. The `and` operator was done
with SIMD instructions, so one instruction can apply the `and` operation to
eight `int32_t` values.

```text
perf annotate after the applyToSelected loop-end fix:

Key vectorized mask sequence:
  5.59%  vmovdqu      (%rdx,%rax,1),%xmm2
  4.61%  vinserti128  $0x1,0x10(%rdx,%rax,1),%ymm2,%ymm0
 12.41%  vpand        %ymm0,%ymm1,%ymm0
  7.18%  vmovdqu      %xmm0,(%rdx,%rax,1)
  4.79%  vextracti128 $0x1,%ymm0,0x10(%rdx,%rax,1)
  9.66%  add          $0x20,%rax
  4.61%  cmp          %rax,%rcx

Observation:
  The signed-bit clear became a vectorized AND over eight int32_t values.
```

The simple numeric optimization results are:

```text
Simple numeric optimization results:

Step                            HiveHash##i32  HiveHash##i64  HiveHash##f32  HiveHash##f64
Baseline                        105.67 ms      436.43 ms      113.75 ms      480.54 ms
Avoid dynamic_cast               63.35 ms       57.03 ms       68.60 ms       65.73 ms
No-null flat vector using SIMD   16.78 ms       20.26 ms       22.41 ms       21.43 ms
applyToSelected loop end          7.22 ms       10.19 ms       10.77 ms       11.79 ms
```

## Decimal128

Now let's look at decimal hashing. The hash compute method is:

```cpp
ResultType hash(const NativeType& input, SeedType seed) const {
  if (input == 0) {
    return genSeed(seed);
  }
  NativeType value = input;
  uint8_t newScale = normalizeDecimal(value, scale_);
  return genSeed(seed) + decimalHashCode(value, newScale);
}
```

For decimal type, hash computation is more complex than simple numeric type, so
it is not easy for the compiler to generate SIMD instructions. The benchmark
result for Hive decimal was:

```text
Decimal benchmark before the Decimal128 division optimization:

Case             time/iter   iters/s
HiveHash##d64     64.69 ms   15.46
HiveHash##d128   441.89 ms    2.26
```

It was abnormal that `DECIMAL128` hashing cost about 8x more than `DECIMAL64`,
so we ran `perf` on the test case.

```text
Decimal128 perf result:

Samples: 6K of event 'cpu-clock:uhpppH'
Event count (approx.): 1730000000

Overhead  Command         Shared Object                   Symbol
36.56%    velox_sparksql  libgcc_s.so.1                   __modti3
29.19%    velox_sparksql  velox_sparksql_benchmarks_hash  hiveHashMultiple<TypeKind::HUGEINT>
20.04%    velox_sparksql  velox_sparksql_benchmarks_hash  std::_Function_handler<...>
 9.51%    velox_sparksql  libgcc_s.so.1                   __divti3
 0.65%    velox_sparksql  velox_sparksql_benchmarks_hash  __modti3@plt

Observation:
  __modti3 and __divti3 together account for about half of total CPU time.
```

`__modti3` and `__divti3` cost about 50% of the total CPU time. The
corresponding C++ code was:

```cpp
template <typename InputType>
inline static void stripTrailingZeros(InputType& input, uint8_t& scale) {
  while (std::abs(input) >= 10L && scale > 0) {
    if (bits::isBitSet(&input, 0)) { // odd
      break;
    }
    if (input % 10) { // to be optimized
      break;
    }
    input /= 10;
    // TODO: checkScale
    scale -= 1;
  }
  return;
}
```

The expensive operations are the modulo and division by 10:

```cpp
input % 10;
input /= 10;
```

Notice that we only divide by 10 here, so we can use a simpler way to get the
dividend and remainder of `int128_t / 10`.

The following block is algebra-style C++ pseudocode used to explain the
division-by-10 transformation:

```cpp
// Suppose lower and upper are the lower and upper bits of uint128_t num.
uint64_t upper = 10 * a + b
uint64_t lower = 10 * c + d

// If num > 0:
uint128_t num = upper * 2^64 + lower
    = (10 * a + b) * 2^64 + 10 * c + d
    = 10 * (a * 2^64 + c) + b * 2^64 + d

// So:
num % 10 = (b * 2^64 + d) % 10
         = (b * (2^64 / 10) * 10 + b * (2^64 % 10) + d) % 10
         = (b * 6 + d) % 10
num / 10 = a * 2^64 + c + (b * 2^64 + d) / 10
         = a * 2^64 + c +
           (b * (2^64 / 10) * 10 + b * (2^64 % 10) + d) / 10
         = a * 2^64 + c + b * (2^64 / 10) + (b * 6 + d) / 10

// If num == INT128_MIN:
num % 10 = INT128_MIN % 10
num / 10 = INT128_MIN / 10

// If INT128_MIN < num < 0:
num % 10 = (-num) % 10
num / 10 = -(num / 10)
```

Now we can convert `int128_t` division into `int64_t` arithmetic. The benchmark
result showed that `HiveHash##d128` cost was reduced from 441 ms to 144 ms.

```text
Decimal benchmark after the Decimal128 division optimization:

Case             time/iter   iters/s
HiveHash##d64     60.86 ms   16.43
HiveHash##d128   144.19 ms    6.94
```

## String

For string type, Hive hash is very simple. It incrementally multiplies by 31
for each character:

```cpp
ReturnType hashBytes(
    const StringView& input,
    SeedType seed,
    const TypePtr& inputType) {
  int32_t result = 0;
  size_t size = input.size();
  char* s = input.data();
  for (size_t i = 0; i < size; ++i) {
    result = (result * 31) + s[i];
  }
  return genSeed(seed) + result;
}
```

How do we optimize this procedure? Can we use SIMD instructions to implement
this logic? If we want to use SIMD for string hashing, first we try to unroll
the loop:

```cpp
ReturnType hashBytes(
    const StringView& input,
    SeedType seed,
    const TypePtr& inputType) {
  int32_t result = 0;
  size_t size = input.size();
  char* s = input.data();
  for (size_t i = 0; i < size; i += 8) {
    result = (result * 31) + s[i];
    result = (result * 31) + s[i + 1];
    result = (result * 31) + s[i + 2];
    result = (result * 31) + s[i + 3];
    result = (result * 31) + s[i + 4];
    result = (result * 31) + s[i + 5];
    result = (result * 31) + s[i + 6];
    result = (result * 31) + s[i + 7];
  }
  for (size_t i = size - size % 8; i < size; i++) {
    result = (result * 31) + s[i];
  }
  return genSeed(seed) + result;
}
```

If we combine the loop body into one expression, we get:

```cpp
ReturnType hashBytes(
    const StringView& input,
    SeedType seed,
    const TypePtr& inputType) {
  int32_t result = 0;
  size_t size = input.size();
  char* s = input.data();
  for (size_t i = 0; i < size; i += 8) {
    result =
        ((((((result * 31) + s[i]) * 31 + s[i + 1]) * 31 + s[i + 2]) *
          31 + s[i + 3]) *
          31 + s[i + 4]) *
          31 + s[i + 5]) *
          31 + s[i + 6];
    result = result * 31 + s[i + 7];
    // result = result * 31^8 + s[i] * 31^7 + s[i + 1] * 31^6
    //        + ... + s[i + 7]
  }
  for (size_t i = size - size % 8; i < size; i++) {
    result = (result * 31) + s[i];
  }
  return genSeed(seed) + result;
}
```

We can use vectorized operators to replace this code:

```cpp
ReturnType hashBytes(
    const StringView& input,
    SeedType seed,
    const TypePtr& inputType) {
  int32_t result = 0;
  size_t size = input.size();
  char* s = input.data();
  i32x8 resultVec = <0, 0, ..., 0>;
  for (size_t i = 0; i < size; i += 8) {
    i8x8 strVal = *(i8x8*)(s + i);
    i32x8 inputVal = (i32x8)strVal;
    i32x8 multiplier = <31^7, 31^6, 31^5, ..., 31, 1>;
    resultVec = resultVec * 31^8 + inputVal * multiplier;
  }
  result = horizontalAdd(resultVec);
  for (size_t i = size - size % 8; i < size; i++) {
    result = (result * 31) + s[i];
  }
  return genSeed(seed) + result;
}
```

The multiplier is applied to all `inputVal` lanes and then added to
`resultVec`. According to the distributive property of multiplication, we can
multiply after all additions:

```cpp
ReturnType hashBytes(
    const StringView& input,
    SeedType seed,
    const TypePtr& inputType) {
  int32_t result = 0;
  size_t size = input.size();
  char* s = input.data();
  i32x8 resultVec = <0, 0, ..., 0>;
  for (size_t i = 0; i < size; i += 8) {
    i8x8 strVal = *(i8x8*)(s + i);
    i32x8 inputVal = (i32x8)strVal;
    resultVec = resultVec * 31^8 + inputVal;
  }
  i32x8 multiplier = <31^7, 31^6, 31^5, ..., 31, 1>;
  resultVec = resultVec * multiplier;
  result = horizontalAdd(resultVec);
  for (size_t i = size - size % 8; i < size; i++) {
    result = (result * 31) + s[i];
  }
  return genSeed(seed) + result;
}
```

Next, we look into each vectorized operation and replace it with x86 SIMD
instructions.

```text
i8x8 strVal = *(i8x8*)(s + i);
i32x8 inputVal = (i32x8)strVal;
```

First, load 8 bytes into an `i64` integer. Then use `_mm_set1_epi64` to save
the `i64` integer into an `i128` register. Finally, use
`_mm256_cvtepu8_epi32` to convert from `i8x8` to `i32x8` with zero extension.

```text
resultVec = resultVec * 31^8 + inputVal;
```

To compute this expression, we need to multiply `i32x8` with `31^8`. However,
AVX2 only supports four `i32` multiplications and stores the results in four
`i64` lanes with `_mm256_mul_epi32`. So we do not use multiply instructions
here; instead, we use shifts and sums to simulate `i32x8 * 31^8`.

First, convert `31^8` to binary:

```text
31^8 = 0b 1100 0110 1001 0100 0100 0100 0110 1111 0000 0001
```

Because we only multiply `i32` with `31^8`, we do not need to care about bits
higher than 32:

```text
a * 31^8
  = a * 0b 1001 0100 0100 0100 0110 1111 0000 0001
  = a * (1 << 31 + 1 << 28 + 1 << 26 + 1 << 22 +
         1 << 18 + 1111111 << 8 - 1 << 12 + 1)
  = a * (1 << 31 + 1 << 28 + 1 << 26 + 1 << 22 +
         1 << 18 + 1 << 15 - 1 << 8 - 1 << 12 + 1)
  = a * ((1 << 3 + 1) << 28 +
         (1 << 4 + 1) << 22 +
         (1 << 3 + 1) << 15 -
         (1 << 4 + 1) << 8 + 1)
```

Suppose:

```text
a0 = (a << 3) + a
a1 = (a << 4) + a
```

Then:

```text
a * 31^8 = (a0 << 28) + (a1 << 22) + (a0 << 15) - (a1 << 8) + a
```

So we can use six shift instructions, five add instructions, and one subtract
instruction to simulate `a * 31^8`. Vectorized shift
(`_mm256_slli_epi32`), add (`_mm256_add_epi32`), and subtract
(`_mm256_sub_epi32`) are all fast instructions, so this procedure is
efficient.

```text
i32x8 multiplier = <31^7, 31^6, 31^5, ..., 31, 1>;
resultVec = resultVec * multiplier;
result = horizontalAdd(resultVec);
```

This part transfers the `i32x8` temporary result into the final hash result.
Since multiply is not efficient in SIMD for this case, we transfer it into
scalar operations:

```cpp
result = 0;
for (int i = 0; i < 8; i++) {
  result = result * 31 + resultVec[i];
}
```

The only SIMD instruction needed here is extracting each element from
`resultVec`, using `_mm_extract_epi32`.

This code still needs eight multiply-add operations. We can first compute an
`i32x4` result vector from the `i32x8` result vector:

```cpp
i32x4 lowerResultVec = resultVec get lower i32x4;
i32x4 upperResultVec = resultVec get upper i32x4;
i32x4 smallerResultVec = lowerResultVec * 31^4 + upperResultVec;
result = 0;
for (int i = 0; i < 4; i++) {
  result = result * 31 + smallerResultVec[i];
}
```

After optimization, the string hash case time was reduced from 569 ms to about
200 ms.

```text
String benchmark after SIMD optimization:

Case            time/iter   iters/s
HiveHash##str   206.50 ms   4.84
```

## Complex Type

Before the refactor, complex types were hashed element by element. For array
type, the code was:

```cpp
inline ResultType hash(const NativeType& input, SeedType seed) const {
  auto [array, index] = input;
  ResultType result = HiveHashBase::genSeed(seed);
  result += SWITCH_TYPE_HASH(
      array->elements()->type(), hashArrayElement, array, index, 0);
  return result;
}

template <TypeKind elementKind>
ResultType hashArrayElement(
    const ArrayVector* array,
    size_t index,
    SeedType seed) {
  ResultType result = seed;
  HiveHash<elementKind> hasher(array->elements()->type());
  if (array->isNullAt(index)) {
    return HiveHashBase::hashNull(seed);
  }
  auto start = array->offsetAt(index);
  auto end = start + array->sizeAt(index);
  for (auto idx = start; idx < end; idx++) {
    if (array->elements()->isNullAt(idx)) {
      result = HiveHashBase::hashNull(result);
    } else {
      result = hasher.hash(
          getValueFromVector<elementKind>(array->elements(), idx), result);
    }
  }
  return result;
}
```

We did type dispatch by the element type for each element. If there was a
nested type in the element type, there would be more type dispatch.

Suppose we think of each nested array value as a tree. Each array value is an
internal node, and primitive values are leaf nodes. The hash of a nested array
value can then be seen as a traversal order of a tree.

```text
Traversal order for nested arrays:

Example value:

             A
          /     \
         B       C
       /  \     / \
     v1   v2   D   v5
             /  \
            v3  v4

Before: postorder-like hashing per nested value

  1. hash(v1)
  2. hash(v2)
  3. hash(B)
  4. hash(v3)
  5. hash(v4)
  6. hash(D)
  7. hash(v5)
  8. hash(C)
  9. hash(A)

After: level-order batch hashing

  deepest primitive level:  hash(v1), hash(v2), hash(v3), hash(v4), hash(v5)
  next array level:         hash(B), hash(D)
  next array level:         hash(C)
  top array level:          hash(A)
```

Before the refactor, we hashed nested array data using postorder traversal. We
first computed the element hash value of one array, then computed that array
data's hash. Then we computed the next array element's hash value, and then the
next array's hash value.

A better compute order is the level-order batch hashing shown above. We compute
the nested array data with level-order traversal. First, compute every primitive
value's hash at the deepest level of the nested array data. Then compute every
array hash value at the next level. This method saves a lot of type dispatch
and function calls when hashing nested array types.

The code is:

```cpp
template <TypeKind typeKind>
__attribute__((noinline)) bool hiveHashMultiple(
    const BaseVector* input,
    HiveHashBase::ResultType* result,
    size_t inputSize,
    bool useDefaultSeed) {
  if constexpr (typeKind == TypeKind::ARRAY) {
    DecodedVector decoded(*input);
    const ArrayVector* arrayVector = decoded.base()->as<const ArrayVector>();
    std::vector<HiveHashBase::ResultType> partialResult(
        arrayVector->elements()->size());
    SWITCH_TYPE_HASH(
        arrayVector->elements()->type(),
        hiveHashMultiple,
        arrayVector->elements().get(),
        partialResult.data(),
        partialResult.size(),
        true);
    for (size_t i = 0; i < inputSize; i++) {
      HiveHashBase::SeedType seed = useDefaultSeed ? 0 : result[i];
      HiveHashBase::ResultType hashValue = 0;
      if (decoded.isNullAt(i)) {
        hashValue = HiveHashBase::hashNull(seed);
      } else {
        auto index = decoded.index(i);
        auto start = arrayVector->offsetAt(index);
        auto end = start + arrayVector->sizeAt(index);
        HiveHashBase::ResultType tempResult = 0;
        for (auto idx = start; idx < end; idx++) {
          tempResult = HiveHashBase::genSeed(tempResult) + partialResult[idx];
        }
        hashValue = HiveHashBase::genSeed(seed) + tempResult;
      }
      result[i] = hashValue;
    }
  } else {
    // Other type hasher.
  }
  return true;
}
```

For map and struct types, we can also change the compute order to level-order
traversal like arrays. For map, first compute the hash values of all keys and
values, then compute the hash value of all maps. For struct, compute the hash
value of all columns, then combine the hash value of all struct rows at a time.

## Conclusion

After all optimizations were applied, we got the final benchmark result:

```text
Final HiveHash benchmark:

Case                         time/iter   iters/s
HiveHash##i32                  7.09 ms   140.97
HiveHash##i64                 10.05 ms    99.54
HiveHash##f32                 10.65 ms    93.90
HiveHash##f64                 11.71 ms    85.43
HiveHash##d64                 67.87 ms    14.73
HiveHash##d128               150.68 ms     6.64
HiveHash##str                204.67 ms     4.89
HiveHash##row_i32_i32         27.45 ms    36.43
HiveHash##row_i64_i64         33.33 ms    30.00
HiveHash##row_str_str        424.34 ms     2.36
HiveHash##array_i32          169.15 ms     5.91
HiveHash##array_i64          196.05 ms     5.10
HiveHash##array_str            2.29 s    436.59 m
HiveHash##map_i32_i32        120.35 ms     8.31
HiveHash##map_i64_i64        178.20 ms     5.61
HiveHash##map_str_str          4.27 s    234.06 m
```

Detailed benchmark comparison before and after the refactor:

```text
Detailed comparison:

Case                         Before      After       Improvement
INTEGER                      105.67 ms     7.09 ms   13.9x
BIGINT                       436.43 ms    10.05 ms   42.43x
REAL                         113.75 ms    10.65 ms   9.68x
DOUBLE                       480.54 ms    11.72 ms   40x
DECIMAL64                    401.60 ms    67.87 ms   4.92x
DECIMAL128                   772.65 ms   150.68 ms   4.13x
STRING                       569.61 ms   204.67 ms   1.78x
STRUCT<INTEGER, INTEGER>     351.02 ms    27.45 ms   11.79x
STRUCT<BIGINT, BIGINT>      1000.00 ms    33.33 ms   29x
STRUCT<STRING, STRING>      1250.00 ms   424.34 ms   1.95x
ARRAY<INTEGER>               501.93 ms   169.15 ms   1.97x
ARRAY<BIGINT>               3480.00 ms   196.05 ms   16.75x
ARRAY<STRING>               4670.00 ms  2290.00 ms   1.04x
MAP<INTEGER, INTEGER>       1510.00 ms   120.35 ms   11.55x
MAP<BIGINT, BIGINT>         7580.00 ms   178.20 ms   41.54x
MAP<STRING, STRING>        10720.00 ms  4270.00 ms   1.51x
```

For the Spark SQL jobs mentioned at the beginning, HiveHash total time was
reduced from 65.71 hours to 1.30 hours, and from 154.51 hours to 11.96 hours.
Performance improved by 49.5x and 12.9x respectively.

```text
After the refactor:

Project 1:
  HiveHash total time: 65.71 h -> 1.30 h
  Improvement:         49.5x

Project 2:
  HiveHash total time: 154.51 h -> 11.96 h
  Improvement:         12.9x
```

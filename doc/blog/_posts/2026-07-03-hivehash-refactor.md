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

![Spark shuffle hash profile]({{ '/assets/images/hivehash-refactor/image-18.png' | relative_url }})

The key observation was:

- The first project's hash input types were three `BIGINT` columns.
- The second project's hash input types were three `BIGINT` columns and one
  string column.

This suggested that Bolt might have performance issues when hashing `BIGINT`
and string values.

## Benchmark Test

We then ran benchmark tests. The result showed that, compared with hashing
`i32`, hashing `i64` cost about 4x more.

![Initial HiveHash benchmark]({{ '/assets/images/hivehash-refactor/image-1.png' | relative_url }})

## Bigint, Decimal64, and Double

The HiveHash `i64` implementation was:

![HiveHash i64 code]({{ '/assets/images/hivehash-refactor/image-5.png' | relative_url }})

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

![Numeric benchmark after avoiding dynamic casts]({{ '/assets/images/hivehash-refactor/image-4.png' | relative_url }})

`HiveHash##i64` and `HiveHash##f64` became much faster, moving from more than
400 ms to about 60 ms.

If we use `perf` on `HiveHash##i64`, we can see that hashing is done with
simple shift and subtract instructions, without SIMD instructions.

![HiveHash i64 perf after dynamic-cast removal]({{ '/assets/images/hivehash-refactor/image-9.png' | relative_url }})

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

![Flat vector no-null benchmark]({{ '/assets/images/hivehash-refactor/image-14.png' | relative_url }})

Case time went from more than 60 ms to about 20 ms. Looking at the perf result,
the HiveHash computation was generated with good SIMD instructions.

![SIMD perf result 1]({{ '/assets/images/hivehash-refactor/image-21.png' | relative_url }})

![SIMD perf result 2]({{ '/assets/images/hivehash-refactor/image-17.png' | relative_url }})

However, from the perf result above, the top time-consuming function became
`HashFunctionEvaluator::apply`. Looking at the detailed instructions:

![HashFunctionEvaluator apply perf]({{ '/assets/images/hivehash-refactor/image-15.png' | relative_url }})

We can locate the corresponding C++ code. After the HiveHash result is
returned, Bolt clears the signed bit of all results.

![Clear signed bit code]({{ '/assets/images/hivehash-refactor/image-13.png' | relative_url }})

Why was this not done with SIMD instructions? Inspecting `applyToSelected`, we
found that it has a separate path for all selected rows. The loop end used a
member variable, `end_`, so the compiler had to call `func` one by one in case
`end_` was reassigned.

![applyToSelected before loop-end fix]({{ '/assets/images/hivehash-refactor/image-3.png' | relative_url }})

To fix this, we simply assigned `end_` to a local `const` variable:

![applyToSelected after loop-end fix]({{ '/assets/images/hivehash-refactor/image-11.png' | relative_url }})

After this change, the benchmark result became:

![Benchmark after applyToSelected loop-end fix]({{ '/assets/images/hivehash-refactor/image-20.png' | relative_url }})

The corresponding instructions are shown below. The `and` operator was done
with SIMD instructions, so one instruction can apply the `and` operation to
eight `int32_t` values.

![Vectorized and instruction]({{ '/assets/images/hivehash-refactor/image-8.png' | relative_url }})

The simple numeric optimization results are:

| Step | `HiveHash##i32` | `HiveHash##i64` | `HiveHash##f32` | `HiveHash##f64` |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 105.67 ms | 436.43 ms | 113.75 ms | 480.54 ms |
| Avoid `dynamic_cast` | 63.35 ms | 57.03 ms | 68.60 ms | 65.73 ms |
| No-null flat vector using SIMD | 16.78 ms | 20.26 ms | 22.41 ms | 21.43 ms |
| `applyToSelected` loop end | 7.22 ms | 10.19 ms | 10.77 ms | 11.79 ms |

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

![Decimal benchmark baseline]({{ '/assets/images/hivehash-refactor/image-19.png' | relative_url }})

It was abnormal that `DECIMAL128` hashing cost about 8x more than `DECIMAL64`,
so we ran `perf` on the test case.

![Decimal128 perf]({{ '/assets/images/hivehash-refactor/image-2.png' | relative_url }})

`__modti3` and `__divti3` cost about 50% of the total CPU time. The
corresponding C++ code was:

![Decimal normalization code]({{ '/assets/images/hivehash-refactor/image-10.png' | relative_url }})

Notice that we only divide by 10 here, so we can use a simpler way to get the
dividend and remainder of `int128_t / 10`.

```cpp
// Suppose lower and upper are the lower and upper bits of uint128_t num.
uint64_t upper = 10 * a + b;
uint64_t lower = 10 * c + d;

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

![Decimal128 benchmark after division optimization]({{ '/assets/images/hivehash-refactor/image-6.png' | relative_url }})

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

![String benchmark after optimization]({{ '/assets/images/hivehash-refactor/image-12.png' | relative_url }})

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

![Complex type traversal order]({{ '/assets/images/hivehash-refactor/image.png' | relative_url }})

Before the refactor, we hashed nested array data using postorder traversal. We
first computed the element hash value of one array, then computed that array
data's hash. Then we computed the next array element's hash value, and then the
next array's hash value.

A better compute order is shown on the right side of the figure. We compute the
nested array data with level-order traversal. First, compute every primitive
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

![Final benchmark result]({{ '/assets/images/hivehash-refactor/image-16.png' | relative_url }})

Detailed benchmark comparison before and after the refactor:

| Case | Before | After | Performance improvement |
| --- | ---: | ---: | ---: |
| `INTEGER` | 105.67 ms | 7.09 ms | 13.9x |
| `BIGINT` | 436.43 ms | 10.05 ms | 42.43x |
| `REAL` | 113.75 ms | 10.65 ms | 9.68x |
| `DOUBLE` | 480.54 ms | 11.72 ms | 40x |
| `DECIMAL64` | 401.60 ms | 67.87 ms | 4.92x |
| `DECIMAL128` | 772.65 ms | 150.68 ms | 4.13x |
| `STRING` | 569.61 ms | 204.67 ms | 1.78x |
| `STRUCT<INTEGER, INTEGER>` | 351.02 ms | 27.45 ms | 11.79x |
| `STRUCT<BIGINT, BIGINT>` | 1000 ms | 33.33 ms | 29x |
| `STRUCT<STRING, STRING>` | 1250 ms | 424.34 ms | 1.95x |
| `ARRAY<INTEGER>` | 501.93 ms | 169.15 ms | 1.97x |
| `ARRAY<BIGINT>` | 3480 ms | 196.05 ms | 16.75x |
| `ARRAY<STRING>` | 4670 ms | 2290 ms | 1.04x |
| `MAP<INTEGER, INTEGER>` | 1510 ms | 120.35 ms | 11.55x |
| `MAP<BIGINT, BIGINT>` | 7580 ms | 178.20 ms | 41.54x |
| `MAP<STRING, STRING>` | 10720 ms | 4270 ms | 1.51x |

For the Spark SQL jobs mentioned at the beginning, HiveHash total time was
reduced from 65.71 hours to 1.30 hours, and from 154.51 hours to 11.96 hours.
Performance improved by 49.5x and 12.9x respectively.

![Spark workload after HiveHash refactor]({{ '/assets/images/hivehash-refactor/image-7.png' | relative_url }})

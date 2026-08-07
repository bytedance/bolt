/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Pre-built JIT kernels — compiled to LLVM bitcode at build time.
// Compiled with bolt headers so kernels can use bolt types (DecodedVector,
// etc.) directly. Inline methods from bolt headers get compiled into the
// bitcode, avoiding virtual dispatch at JIT runtime.

#include <cstdint>
#include <cstring>
#include <limits>

#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/DecodedVector.h"

using bytedance::bolt::DecodedVector;
using bytedance::bolt::HugeInt;
using bytedance::bolt::int128_t;
using bytedance::bolt::Timestamp;

extern "C" {

// PoC: simple add function to validate the prebuilt IR pipeline.
__attribute__((always_inline)) int8_t jit_prebuilt_add(int8_t a, int8_t b) {
  return a + b;
}

// ============================================================================
// Store kernels — store values from DecodedVector into RowContainer rows.
//
// Fixed-width types: fully inlined (DecodedVector methods are inline).
// Variable-width types (StringView, complex): extern call to RowContainer
// (resolved at JIT link time via process symbol table).
//
// Sentinel values on null match RowContainer::storeWithNulls.
// ============================================================================

// --- Fixed-width arithmetic types (fully inlined) ---

#define DEFINE_STORE_KERNEL(name, T, sentinel)                          \
  __attribute__((always_inline)) void name(                             \
      char* row,                                                        \
      int32_t offset,                                                   \
      const DecodedVector* decoded,                                     \
      int32_t index,                                                    \
      int32_t nullByte,                                                 \
      int8_t nullMask) {                                                \
    if (decoded->isNullAt(index)) {                                     \
      row[nullByte] |= nullMask;                                        \
      *reinterpret_cast<T*>(row + offset) = sentinel;                   \
    } else {                                                            \
      *reinterpret_cast<T*>(row + offset) = decoded->valueAt<T>(index); \
    }                                                                   \
  }

DEFINE_STORE_KERNEL(jit_store_i8, int8_t, std::numeric_limits<int8_t>::max())
DEFINE_STORE_KERNEL(jit_store_i16, int16_t, std::numeric_limits<int16_t>::max())
DEFINE_STORE_KERNEL(jit_store_i32, int32_t, std::numeric_limits<int32_t>::max())
DEFINE_STORE_KERNEL(jit_store_i64, int64_t, std::numeric_limits<int64_t>::max())
DEFINE_STORE_KERNEL(jit_store_f32, float, std::numeric_limits<float>::max())
DEFINE_STORE_KERNEL(jit_store_f64, double, std::numeric_limits<double>::max())

#undef DEFINE_STORE_KERNEL

// --- HUGEINT (int128_t): memcpy-based, fully inlined ---

__attribute__((always_inline)) void jit_store_i128(
    char* row,
    int32_t offset,
    const DecodedVector* decoded,
    int32_t index,
    int32_t nullByte,
    int8_t nullMask) {
  if (decoded->isNullAt(index)) {
    row[nullByte] |= nullMask;
    memset(row + offset, 0, sizeof(int128_t));
  } else {
    HugeInt::serialize(decoded->valueAt<int128_t>(index), row + offset);
  }
}

// --- TIMESTAMP: struct copy, fully inlined ---

__attribute__((always_inline)) void jit_store_ts(
    char* row,
    int32_t offset,
    const DecodedVector* decoded,
    int32_t index,
    int32_t nullByte,
    int8_t nullMask) {
  if (decoded->isNullAt(index)) {
    row[nullByte] |= nullMask;
    *reinterpret_cast<Timestamp*>(row + offset) = Timestamp();
  } else {
    *reinterpret_cast<Timestamp*>(row + offset) =
        decoded->valueAt<Timestamp>(index);
  }
}

// --- VARCHAR and Complex types (ARRAY/MAP/ROW) ---
// VARCHAR and complex types (ARRAY/MAP/ROW) need RowContainer access
// (HashStringAllocator for VARCHAR, ContainerRowSerde for complex types).
// We can't include RowContainer.h here because clang (used for bitcode
// compilation) has incompatibilities with the deep header chain
// (DecimalUtil, folly/hash, type_traits — __int128 make_unsigned,
// ambiguous to_chars, etc.). These types fall back to an extern call
// to RowContainer::store(), resolved at JIT link time via the process
// symbol table. See jit_store_row_column in PrebuiltStoreBenchmark.cpp.

} // extern "C"

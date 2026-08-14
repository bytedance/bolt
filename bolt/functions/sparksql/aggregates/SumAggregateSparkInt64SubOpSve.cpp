/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 *
 * AArch64 SVE batch kernel for Spark sum(bigint) HashAgg group updates.
 * `updateGroupsFromDecoded` (member) builds glue structs and calls
 * `sveHashAggBatchUpdateGroupSums` (anonymous namespace kernel).
 * Compiled only on aarch64 (see aggregates/CMakeLists.txt, `-march=armv8-a+sve`).
 */

#include "bolt/functions/sparksql/aggregates/SumAggregateSparkInt64SubOp.h"

#include <arm_sve.h>

#if defined(__linux__)
#include <sys/auxv.h>
#endif

#include "bolt/vector/BaseVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/SelectivityVector.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

constexpr uint64_t kSupportedSveVectorBytes = 32;

/// Test one bit in a packed bitmap (used by `isBitNull` for constant-null mode).
template <typename T>
inline bool isBitSet(const T* bits, uint64_t idx) {
  return bits[idx / (sizeof(bits[0]) * 8)] &
      (static_cast<T>(1) << (idx & ((sizeof(bits[0]) * 8) - 1)));
}

/// True when bit `index` is cleared (Velox/Bolt null-bit convention).
inline bool isBitNull(const uint64_t* bits, int32_t index) {
  return isBitSet(bits, index) == false;
}
/// Round `value` up to a multiple of `factor` (32-row chunk alignment).
template <typename T, typename U>
constexpr inline T roundUp(T value, U factor) {
  return (value + (factor - 1)) / factor * factor;
}

/// HashAgg group write side: clear null flags and update int64 accumulators.
struct HashAggGroupSink {
  int32_t nullByte;
  uint8_t nullMask;
  uint64_t* numNulls;
  char** groups;
};

/// Active rows (`SelectivityVector`) plus decoded values (`BatchReadView`).
struct SelectedBatchReadView {
  const ::bytedance::bolt::SelectivityVector& rows;
  ::bytedance::bolt::BatchReadView readView;
};

/// Builds an 8-byte-lane predicate for input nulls per `nullsMode`
/// (`BatchReadView::nullsMode`). Used to form `inputNullMask` in the main kernel.
svbool_t sveInputNullMaskForMode(
    const uint8_t* nulls_,
    int32_t index,
    int nullsMode,
    const uint32_t* dictIndices,
    int32_t length) {
  svbool_t pg;
  if (nullsMode == 0) {
    pg = svptrue_b8();
    return pg;
  } else if (nullsMode == 1) {
    __asm__ __volatile__("ldr %0, [%1]"
                         : "=Upl"(pg)
                         : "r"(&(nulls_[index]))
                         : "memory");
    return pg;
  } else if (nullsMode == 2) {
    if (!isBitNull(
            reinterpret_cast<const uint64_t*>(nulls_),
            0))
    {
      pg = svptrue_b8();
    } else {
      pg = svpfalse();
    }
    return pg;
  } else if (nullsMode == 3) {

    svuint32_t onc = svdup_u32(1);
    svuint32_t inv = svindex_u32(0, 1);
    svuint32_t pow = svlsl_m(svptrue_b32(), onc, inv);
    uint8_t tmpNulls[4] = {0};
    const uint32_t* null32ptr = reinterpret_cast<const uint32_t*>(nulls_);

    svuint32_t posv, idxbufv, bufv, offsetv;
    svbool_t nullvec, pg1;

    // nullsMode==3: pack null bits for eight dictionary lanes (chunk 0).
    pg1 = svwhilelt_b32(index * 8, length);
    posv = svld1(pg1, dictIndices + index * 8);
    idxbufv = svlsr_x(pg1, posv, 5); // u32 word index (pos / 32)
    bufv = svld1_gather_index(pg1, null32ptr, idxbufv);
    offsetv = svand_m(pg1, posv, 0b11111); // bit index within the u32 word
    bufv = svlsr_m(pg1, bufv, offsetv);
    bufv = svand_m(pg1, bufv, 0x1);
    nullvec = svcmpgt(pg1, bufv, 0);
    if (__builtin_expect((svptest_any(pg1, nullvec)), 0)) {
      uint8_t nullsres = svaddv(nullvec, pow);
      tmpNulls[0] = nullsres;
    } else {
      tmpNulls[0] = 0;
    }

    // nullsMode==3: dictionary null bits (chunk 1).
    pg1 = svwhilelt_b32(index * 8 + 8, length);
    posv = svld1(pg1, dictIndices + index * 8 + 8);
    idxbufv = svlsr_x(pg1, posv, 5);
    bufv = svld1_gather_index(pg1, null32ptr, idxbufv);
    offsetv = svand_m(pg1, posv, 0b11111);
    bufv = svlsr_m(pg1, bufv, offsetv);
    bufv = svand_m(pg1, bufv, 0x1);
    nullvec = svcmpgt(pg1, bufv, 0);
    if (__builtin_expect((svptest_any(pg1, nullvec)), 0)) {
      uint8_t nullsres = svaddv(nullvec, pow);
      tmpNulls[1] = nullsres;
    } else {
      tmpNulls[1] = 0;
    }

    // nullsMode==3: dictionary null bits (chunk 2).
    pg1 = svwhilelt_b32(index * 8 + 16, length);
    posv = svld1(pg1, dictIndices + index * 8 + 16);
    idxbufv = svlsr_x(pg1, posv, 5);
    bufv = svld1_gather_index(pg1, null32ptr, idxbufv);
    offsetv = svand_m(pg1, posv, 0b11111);
    bufv = svlsr_m(pg1, bufv, offsetv);
    bufv = svand_m(pg1, bufv, 0x1);
    nullvec = svcmpgt(pg1, bufv, 0);
    if (__builtin_expect((svptest_any(pg1, nullvec)), 0)) {
      uint8_t nullsres = svaddv(nullvec, pow);
      tmpNulls[2] = nullsres;
    } else {
      tmpNulls[2] = 0;
    }

    // nullsMode==3: dictionary null bits (chunk 3).
    pg1 = svwhilelt_b32(index * 8 + 24, length);
    posv = svld1(pg1, dictIndices + index * 8 + 24);
    idxbufv = svlsr_x(pg1, posv, 5);
    bufv = svld1_gather_index(pg1, null32ptr, idxbufv);
    offsetv = svand_m(pg1, posv, 0b11111);
    bufv = svlsr_m(pg1, bufv, offsetv);
    bufv = svand_m(pg1, bufv, 0x1);
    nullvec = svcmpgt(pg1, bufv, 0);
    if (__builtin_expect((svptest_any(pg1, nullvec)), 0)) {
      uint8_t nullsres = svaddv(nullvec, pow);
      tmpNulls[3] = nullsres;
    } else {
      tmpNulls[3] = 0;
    }

    __asm__ __volatile__("ldr %0, [%1]"
                         : "=Upl"(pg)
                         : "r"(tmpNulls)
                         : "memory");
    return pg;
  }
  // Unknown nullsMode: inactive predicate.
  pg = svpfalse();
  return pg;
}

/// Among four loaded group pointers, keep only lanes whose pointer differs from
/// earlier lanes in the quad (HashAgg duplicate-group guard within one SVE step).
inline __attribute__((always_inline)) svbool_t
sveMaskDistinctGroupPtrs(svbool_t pg, const svuint64_t val) {
  svuint64_t s1 = svext_u64(val, val, 1);
  svbool_t mask2 = svcmpeq(svwhilelt_b64(0, 3), val, s1);

  svuint64_t s2 = svext_u64(val, val, 2);
  svbool_t mask3 = svcmpeq(svwhilelt_b64(0, 2), val, s2);
  svbool_t mask12 = svorr_b_z(pg, mask2, mask3);

  svuint64_t s3 = svext_u64(val, val, 3);
  svbool_t mask4 = svcmpeq(svwhilelt_b64(0, 1), val, s3);

  svbool_t mask = svorr_b_z(pg, mask4, mask12);
  mask = svnot_b_z(pg, mask);

  return mask;
}

/// Clears accumulator null flags on groups about to receive a non-null update.
/// Vectorized counterpart of `Aggregate::clearNullFlags` for active SVE lanes.
static bool sveClearGroupNullFlags(
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* numNulls,
    svuint64_t ptr,
    svbool_t pg) {
  if (*numNulls) {
    svint64_t group =
        svld1sb_gather_u64base_offset_s64(pg, ptr, nullByte);
    svuint8_t group8 = svreinterpret_u8(group);

    svuint8_t tmp = svand_n_u8_z(pg, group8, nullMask);
    svbool_t test = svcmpne_n_u8(svptrue_b8(), tmp, 0);
    if (svptest_any(svptrue_b8(), test)) {
      uint8_t negNull = ~nullMask;

      svuint8_t adjust = svand_n_u8_m(test, group8, negNull);
      svst1b_scatter_u64base_offset_s64(
          pg, ptr, nullByte, svreinterpret_s64(adjust));

      int num = svcntp_b8(test, test);
      *numNulls -= num;
      return true;
    }
  }
  return false;
}

/// Scalar tail: add input values into group sums for up to four rows. `flag[i]`
/// comes from unpacking an SVE lane mask (`mask20` etc.); no SVE intrinsics here.
template <typename GetAccumPtr>
inline void accumulateGroupSumsFromLaneFlags(
    int indicesMode,
    int64_t constantValue,
    const int64_t* values,
    const uint32_t* dictIndices,
    const uint8_t* flag,
    int32_t rowBase,
    char** groups,
    GetAccumPtr&& getAccumPtr) {
  for (int i = 0; i < 4; ++i) {
    if (flag[i] == 0) {
      continue;
    }
    const int32_t row = rowBase + i;
    int64_t rowValue;
    if (indicesMode == 3) {
      rowValue = values[dictIndices[row]];
    } else if (indicesMode == 2) {
      rowValue = constantValue;
    } else {
      rowValue = values[row];
    }
    *getAccumPtr(*(groups + row)) += rowValue;
  }
}

/// HashAgg Spark sum(bigint) SVE batch kernel: 32-row blocks, group-pointer
/// dedup, null-flag clearing, and per-lane scalar accumulation.
template <typename GetPtr>
static void sveHashAggBatchUpdateGroupSums(
    const HashAggGroupSink& sink,
    const SelectedBatchReadView& input,
    GetPtr&& getAccumPtr) {
  const auto& readView = input.readView;
  const int32_t begin = static_cast<int32_t>(input.rows.begin());
  const int32_t end = static_cast<int32_t>(input.rows.end());
  const int32_t nullsMode = readView.nullsMode;
  const int32_t indicesMode = readView.indicesMode;
  const auto* values = static_cast<const int64_t*>(readView.data);
  const auto* dictIndices = indicesMode == 3
      ? reinterpret_cast<const uint32_t*>(readView.indices)
      : nullptr;
  const uint8_t* rowBits8 =
      reinterpret_cast<const uint8_t*>(input.rows.allBits());
  const uint8_t* inputNullBits8 = readView.nulls != nullptr
      ? reinterpret_cast<const uint8_t*>(readView.nulls)
      : nullptr;
  char** groups = sink.groups;

  int32_t firstWord =
      roundUp(begin, 32) == begin ? begin : roundUp(begin, 32) - 32;
  int32_t lastWord = roundUp(end, 32);
  svbool_t mask, rowMask;
  const int64_t constantValue =
      indicesMode == 2 ? values[readView.constantIndex] : 0;
  // Process 32 logical rows per iteration; `count` is the row index.
  for (int32_t count = firstWord; count + 32 <= lastWord; count += 32) {
    int32_t arr8Index = count / 8;
    svbool_t inputNullMask;
    if (inputNullBits8 != nullptr) {
      inputNullMask = sveInputNullMaskForMode(
          inputNullBits8, arr8Index, nullsMode, dictIndices, end);
    } else {
      inputNullMask = svptrue_b8();
    }
    __asm__ __volatile__("ldr %0, [%1]"
                         : "=Upl"(rowMask)
                         : "r"(&rowBits8[arr8Index])
                         : "memory");
    mask = svand_b_z(svptrue_b8(), rowMask, inputNullMask);
    mask = svand_b_z(svptrue_b8(), mask, svwhilelt_b8(count, end));
    if (!svptest_any(svptrue_b8(), mask)) {
      continue;
    }

    svbool_t mask00 = svunpklo(mask);
    svbool_t mask01 = svunpkhi(mask);
    if (svptest_any(svptrue_b16(), mask00)) {
      svbool_t mask10 = svunpklo(mask00);
      if (svptest_any(svptrue_b32(), mask10)) {
        svbool_t mask20 = svunpklo(mask10);
        svbool_t mask21 = svunpkhi(mask10);
        if (svptest_any(svptrue_b64(), mask20)) {
          svuint64_t ptr =
              svld1(mask20, reinterpret_cast<uint64_t*>(groups + count));
          svbool_t m20 = sveMaskDistinctGroupPtrs(mask20, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m20);
          uint8_t flag0[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag0[0]), "Upl" (mask20) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag0, count, groups, getAccumPtr);
        }

        if (svptest_any(svptrue_b64(), mask21)) {
          svuint64_t ptr =
              svld1(mask21, reinterpret_cast<uint64_t*>(groups + count + 4));
          svbool_t m21 = sveMaskDistinctGroupPtrs(mask21, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m21);
          uint8_t flag1[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag1[0]), "Upl" (mask21) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag1, count + 4, groups, getAccumPtr);
        }
      }
      svbool_t mask11 = svunpkhi(mask00);
      if (svptest_any(svptrue_b32(), mask11)) {
        svbool_t mask22 = svunpklo(mask11);
        svbool_t mask23 = svunpkhi(mask11);
        if (svptest_any(svptrue_b64(), mask22)) {
          svuint64_t ptr =
              svld1(mask22, reinterpret_cast<uint64_t*>(groups + count + 8));
          svbool_t m22 = sveMaskDistinctGroupPtrs(mask22, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m22);
          uint8_t flag2[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag2[0]), "Upl" (mask22) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag2, count + 8, groups, getAccumPtr);
        }

        if (svptest_any(svptrue_b64(), mask23)) {
          svuint64_t ptr =
              svld1(mask23, reinterpret_cast<uint64_t*>(groups + count + 12));
          svbool_t m23 = sveMaskDistinctGroupPtrs(mask23, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m23);
          uint8_t flag3[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag3[0]), "Upl" (mask23) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag3, count + 12, groups, getAccumPtr);
        }
      }
    }

    svbool_t mask12 = svunpklo(mask01);

    if (svptest_any(svptrue_b16(), mask01)) {
      svbool_t mask24 = svunpklo(mask12);
      svbool_t mask25 = svunpkhi(mask12);
      if (svptest_any(svptrue_b32(), mask12)) {
        if (svptest_any(svptrue_b64(), mask24)) {
          svuint64_t ptr =
              svld1(mask24, reinterpret_cast<uint64_t*>(groups + count + 16));
          svbool_t m24 = sveMaskDistinctGroupPtrs(mask24, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m24);
          uint8_t flag4[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag4[0]), "Upl" (mask24) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag4, count + 16, groups, getAccumPtr);
        }

        if (svptest_any(svptrue_b64(), mask25)) {
          svuint64_t ptr =
              svld1(mask25, reinterpret_cast<uint64_t*>(groups + count + 20));
          svbool_t m25 = sveMaskDistinctGroupPtrs(mask25, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m25);
          uint8_t flag5[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag5[0]), "Upl" (mask25) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag5, count + 20, groups, getAccumPtr);
        }
      }
      svbool_t mask13 = svunpkhi(mask01);

      if (svptest_any(svptrue_b32(), mask13)) {
        svbool_t mask26 = svunpklo(mask13);
        svbool_t mask27 = svunpkhi(mask13);
        if (svptest_any(svptrue_b64(), mask26)) {
          svuint64_t ptr =
              svld1(mask26, reinterpret_cast<uint64_t*>(groups + count + 24));
          svbool_t m26 = sveMaskDistinctGroupPtrs(mask26, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m26);
          uint8_t flag6[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag6[0]), "Upl" (mask26) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag6, count + 24, groups, getAccumPtr);
        }

        if (svptest_any(svptrue_b64(), mask27)) {
          svuint64_t ptr =
              svld1(mask27, reinterpret_cast<uint64_t*>(groups + count + 28));
          svbool_t m27 = sveMaskDistinctGroupPtrs(mask27, ptr);
          sveClearGroupNullFlags(
              sink.nullByte, sink.nullMask, sink.numNulls, ptr, m27);
          uint8_t flag7[4] = {0, 0, 0, 0};
          __asm__ __volatile__("str %1, [%0]": : "r" (&flag7[0]), "Upl" (mask27) : "memory");
          
          accumulateGroupSumsFromLaneFlags(
              indicesMode, constantValue, values, dictIndices, flag7, count + 28, groups, getAccumPtr);
        }
      }
    }
  }
}

} // namespace

bool SumAggregateSparkInt64SubOp::updateGroupsFromDecoded(
    char** groups,
    const SelectivityVector& rows,
    ::bytedance::bolt::DecodedVector& decoded) {
  using ::bytedance::bolt::functions::aggregate::Overflow;
  BOLT_DCHECK(Overflow);
  BOLT_DCHECK(sumInt64SubOpCanUseSveKernel());

  const auto readView = decoded.batchReadView();
  BOLT_DCHECK(readView.isReady());

  auto getAccum = [this](char* group) -> int64_t* {
    return this->template value<int64_t>(group);
  };

  sveHashAggBatchUpdateGroupSums(
      HashAggGroupSink{nullByte_, nullMask_, &numNulls_, groups},
      SelectedBatchReadView{rows, readView},
      getAccum);
  return true;
}

bool sumInt64SubOpCanUseSveKernel() {
  static const bool kCanUse = []() {
#if defined(__linux__)
#ifndef HWCAP_SVE
    constexpr unsigned long kBoltHwcapSve = 1UL << 22;
#else
    constexpr unsigned long kBoltHwcapSve = HWCAP_SVE;
#endif
    const unsigned long hwcap = getauxval(AT_HWCAP);
    if ((hwcap & kBoltHwcapSve) == 0) {
      return false;
    }
#endif
    return svcntb() == kSupportedSveVectorBytes;
  }();
  return kCanUse;
}

} // namespace bytedance::bolt::functions::aggregate::sparksql

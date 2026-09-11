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
 */

// Adapted from Apache Arrow.

#include "bolt/dwio/parquet/arrow/LevelConversion.h"

#include <algorithm>
#include <limits>
#include <optional>

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/dwio/parquet/arrow/Exception.h"

#include "bolt/dwio/parquet/arrow/LevelComparison.h"
#define PARQUET_IMPL_NAMESPACE standard
#include "bolt/dwio/parquet/arrow/LevelConversionInc.h"
#undef PARQUET_IMPL_NAMESPACE
namespace bytedance::bolt::parquet::arrow {
namespace {

using ::arrow::internal::CpuInfo;
using ::std::optional;

optional<::arrow::internal::FirstTimeBitmapWriter> makeBitmapWriter(
    ValidityBitmapInputOutput* output,
    int64_t values_to_skip = 0) {
  if (output->valid_bits == nullptr) {
    return std::nullopt;
  }
  return ::arrow::internal::FirstTimeBitmapWriter(
      output->valid_bits,
      output->valid_bits_offset + values_to_skip,
      std::max<int64_t>(output->values_read_upper_bound - values_to_skip, 0));
}

void writeValidityBit(
    optional<::arrow::internal::FirstTimeBitmapWriter>& writer,
    ValidityBitmapInputOutput* output,
    bool isValid) {
  if (!writer.has_value()) {
    return;
  }
  if (isValid) {
    writer->Set();
  } else {
    ++output->null_count;
    writer->Clear();
  }
  writer->Next();
}

int64_t countContinuationRun(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    int64_t start,
    LevelInfo level_info) {
  constexpr int64_t kBatch = xsimd::batch<int16_t>::size;
  const auto repLevel = xsimd::broadcast<int16_t>(level_info.rep_level);
  const auto ancestorDefLevel =
      xsimd::broadcast<int16_t>(level_info.repeated_ancestor_def_level - 1);
  auto runEnd = start + 1;
  for (; runEnd + kBatch <= num_def_levels; runEnd += kBatch) {
    const auto reps = xsimd::load_unaligned(rep_levels + runEnd);
    const auto defs = xsimd::load_unaligned(def_levels + runEnd);
    if (!xsimd::all((reps == repLevel) & (defs > ancestorDefLevel))) {
      break;
    }
  }
  while (runEnd < num_def_levels &&
         rep_levels[runEnd] == level_info.rep_level &&
         def_levels[runEnd] >= level_info.repeated_ancestor_def_level) {
    ++runEnd;
  }
  return runEnd - start;
}

bool isSupportedDirectStructList(
    LevelInfo list_level_info,
    LevelInfo struct_level_info) {
  return struct_level_info.rep_level + 1 == list_level_info.rep_level &&
      struct_level_info.def_level + 2 == list_level_info.def_level &&
      struct_level_info.repeated_ancestor_def_level ==
      list_level_info.repeated_ancestor_def_level;
}

void checkFusedValuesReadUpperBound(
    int64_t values_read_for_bounds,
    const ValidityBitmapInputOutput* list_output,
    const ValidityBitmapInputOutput* struct_output) {
  if (ARROW_PREDICT_FALSE(
          values_read_for_bounds >= list_output->values_read_upper_bound ||
          values_read_for_bounds >= struct_output->values_read_upper_bound)) {
    std::stringstream ss;
    ss << "Definition levels exceeded upper bound: "
       << std::min(
              list_output->values_read_upper_bound,
              struct_output->values_read_upper_bound);
    throw ParquetException(ss.str());
  }
}

template <typename OffsetType>
class OffsetListOutput {
 public:
  explicit OffsetListOutput(OffsetType* offsets)
      : offsets_(offsets), orig_(offsets) {}

  void OnContinuationRun(int64_t run) {
    if (offsets_ == nullptr) {
      return;
    }
    if (ARROW_PREDICT_FALSE(
            run > std::numeric_limits<OffsetType>::max() ||
            *offsets_ > std::numeric_limits<OffsetType>::max() -
                    static_cast<OffsetType>(run))) {
      throw ParquetException("List index overflow.");
    }
    *offsets_ += static_cast<OffsetType>(run);
  }

  void OnNewList(int16_t def_level, const LevelInfo& level_info) {
    if (offsets_ == nullptr) {
      return;
    }
    ++offsets_;
    *offsets_ = *(offsets_ - 1);
    if (def_level >= level_info.def_level) {
      if (ARROW_PREDICT_FALSE(
              *offsets_ == std::numeric_limits<OffsetType>::max())) {
        throw ParquetException("List index overflow.");
      }
      ++*offsets_;
    }
  }

  void Finish() {}

  int64_t valuesRead() const {
    return offsets_ == nullptr ? 0 : offsets_ - orig_;
  }

  int64_t valuesReadForBounds() const {
    return valuesRead();
  }

  bool hasValuesRead() const {
    return offsets_ != nullptr;
  }

 private:
  OffsetType* offsets_;
  OffsetType* orig_;
};

class LengthListOutput {
 public:
  explicit LengthListOutput(
      int32_t* lengths,
      ListLengthsState* state = nullptr,
      bool finalize = true)
      : lengths_(lengths),
        state_(state),
        finalize_(finalize),
        currentLength_(state == nullptr ? 0 : state->current_length),
        haveCurrentList_(state != nullptr && state->has_open_list) {}

  void OnContinuationRun(int64_t run) {
    if (ARROW_PREDICT_FALSE(
            run > std::numeric_limits<int32_t>::max() ||
            currentLength_ > std::numeric_limits<int32_t>::max() - run)) {
      throw ParquetException("List index overflow.");
    }
    currentLength_ += static_cast<int32_t>(run);
  }

  void OnNewList(int16_t def_level, const LevelInfo& level_info) {
    finishCurrentList();
    haveCurrentList_ = true;
    currentLength_ = def_level >= level_info.def_level ? 1 : 0;
  }

  void Finish() {
    if (finalize_) {
      finishCurrentList();
    }
    if (state_ != nullptr) {
      state_->current_length = currentLength_;
      state_->has_open_list = haveCurrentList_;
    }
  }

  int64_t valuesRead() const {
    return valuesRead_;
  }

  int64_t valuesReadForBounds() const {
    return valuesRead_ + (haveCurrentList_ ? 1 : 0);
  }

  bool hasValuesRead() const {
    return true;
  }

 private:
  void finishCurrentList() {
    if (haveCurrentList_) {
      lengths_[valuesRead_++] = currentLength_;
      currentLength_ = 0;
      haveCurrentList_ = false;
    }
  }

  int32_t* lengths_;
  ListLengthsState* state_;
  bool finalize_;
  int64_t valuesRead_{0};
  int32_t currentLength_{0};
  bool haveCurrentList_{false};
};

template <typename ListOutput>
void DefRepLevelsToListInfo(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output,
    ListOutput& list_output,
    int64_t validity_values_to_skip = 0,
    bool finalize_open_list = true) {
  auto valid_bits_writer = makeBitmapWriter(output, validity_values_to_skip);
  for (int64_t x = 0; x < num_def_levels; ++x) {
    // Skip items that belong to empty or null ancestor lists and further nested
    // lists.
    if (def_levels[x] < level_info.repeated_ancestor_def_level ||
        rep_levels[x] > level_info.rep_level) {
      continue;
    }

    if (rep_levels[x] == level_info.rep_level) {
      // A continuation of an existing list. The list output handles whether
      // this extends cumulative offsets or the current list length.
      const auto run = countContinuationRun(
          def_levels, rep_levels, num_def_levels, x, level_info);
      list_output.OnContinuationRun(run);
      x += run - 1;
    } else {
      if (ARROW_PREDICT_FALSE(
              (valid_bits_writer.has_value() &&
               valid_bits_writer->position() + validity_values_to_skip >=
                   output->values_read_upper_bound) ||
              list_output.valuesReadForBounds() >=
                  output->values_read_upper_bound)) {
        std::stringstream ss;
        ss << "Definition levels exceeded upper bound: "
           << output->values_read_upper_bound;
        throw ParquetException(ss.str());
      }

      // current_rep < list rep_level i.e. start of a list (ancestor empty lists
      // are filtered out above). The list output records the new list either as
      // cumulative offsets or as a finalized length for the previous list.
      list_output.OnNewList(def_levels[x], level_info);

      if (valid_bits_writer.has_value()) {
        // the level_info def level for lists reflects element present level.
        // the prior level distinguishes between empty lists.
        if (def_levels[x] >= level_info.def_level - 1) {
          valid_bits_writer->Set();
        } else {
          output->null_count++;
          valid_bits_writer->Clear();
        }
        valid_bits_writer->Next();
      }
    }
  }
  if (ARROW_PREDICT_FALSE(
          finalize_open_list &&
          list_output.valuesReadForBounds() >
              output->values_read_upper_bound)) {
    std::stringstream ss;
    ss << "Definition levels exceeded upper bound: "
       << output->values_read_upper_bound;
    throw ParquetException(ss.str());
  }
  list_output.Finish();
  if (valid_bits_writer.has_value()) {
    valid_bits_writer->Finish();
  }
  if (list_output.hasValuesRead()) {
    output->values_read = list_output.valuesRead();
  } else if (valid_bits_writer.has_value()) {
    output->values_read = valid_bits_writer->position();
  }
  if (output->null_count > 0 && level_info.null_slot_usage > 1) {
    throw ParquetException(
        "Null values with null_slot_usage > 1 not supported."
        "(i.e. FixedSizeLists with null values are not supported)");
  }
}

bool defRepLevelsToListLengthsAndStructBitmap(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo list_level_info,
    LevelInfo struct_level_info,
    ValidityBitmapInputOutput* list_output,
    int32_t* lengths,
    ValidityBitmapInputOutput* struct_output,
    ListLengthsState* state,
    bool finalize) {
  if (!isSupportedDirectStructList(list_level_info, struct_level_info)) {
    return false;
  }

  const auto validityValuesToSkip =
      static_cast<int64_t>(state != nullptr && state->has_open_list);
  auto listValidWriter = makeBitmapWriter(list_output, validityValuesToSkip);
  auto structValidWriter =
      makeBitmapWriter(struct_output, validityValuesToSkip);
  LengthListOutput listLengths(lengths, state, finalize);

  for (int64_t x = 0; x < num_def_levels; ++x) {
    if (def_levels[x] < list_level_info.repeated_ancestor_def_level ||
        rep_levels[x] > list_level_info.rep_level) {
      continue;
    }

    if (rep_levels[x] == list_level_info.rep_level) {
      const auto run = countContinuationRun(
          def_levels, rep_levels, num_def_levels, x, list_level_info);
      listLengths.OnContinuationRun(run);
      x += run - 1;
      continue;
    }

    checkFusedValuesReadUpperBound(
        listLengths.valuesReadForBounds(), list_output, struct_output);
    listLengths.OnNewList(def_levels[x], list_level_info);
    writeValidityBit(
        listValidWriter,
        list_output,
        def_levels[x] >= list_level_info.def_level - 1);
    writeValidityBit(
        structValidWriter,
        struct_output,
        def_levels[x] >= struct_level_info.def_level);
  }
  if (ARROW_PREDICT_FALSE(
          finalize &&
          (listLengths.valuesReadForBounds() >
               list_output->values_read_upper_bound ||
           listLengths.valuesReadForBounds() >
               struct_output->values_read_upper_bound))) {
    std::stringstream ss;
    ss << "Definition levels exceeded upper bound: "
       << std::min(
              list_output->values_read_upper_bound,
              struct_output->values_read_upper_bound);
    throw ParquetException(ss.str());
  }
  listLengths.Finish();

  if (listValidWriter.has_value()) {
    listValidWriter->Finish();
  }
  if (structValidWriter.has_value()) {
    structValidWriter->Finish();
  }
  list_output->values_read = listLengths.valuesRead();
  struct_output->values_read = listLengths.valuesRead();
  if (list_output->null_count > 0 && list_level_info.null_slot_usage > 1) {
    throw ParquetException(
        "Null values with null_slot_usage > 1 not supported."
        "(i.e. FixedSizeLists with null values are not supported)");
  }
  return true;
}

struct AllValidResult {
  int64_t valuesRead{0};
  bool allValid{true};
};

AllValidResult DefLevelsAreAllValidWithRepeatedParent(
    const int16_t* def_levels,
    int64_t num_def_levels,
    LevelInfo level_info) {
  AllValidResult result;
  constexpr int64_t kBatch = xsimd::batch<int16_t>::size;
  const auto ancestorLevel =
      xsimd::broadcast<int16_t>(level_info.repeated_ancestor_def_level - 1);
  const auto validLevel = xsimd::broadcast<int16_t>(level_info.def_level - 1);
  int64_t i = 0;
  for (; i + kBatch <= num_def_levels; i += kBatch) {
    const auto levels = xsimd::load_unaligned(def_levels + i);
    const auto presentMask = simd::toBitMask(levels > ancestorLevel);
    if (presentMask == 0) {
      continue;
    }
    result.valuesRead += __builtin_popcount(presentMask);
    result.allValid &=
        (simd::toBitMask(levels > validLevel) & presentMask) == presentMask;
  }
  for (; i < num_def_levels; ++i) {
    if (def_levels[i] >= level_info.repeated_ancestor_def_level) {
      result.allValid &= def_levels[i] >= level_info.def_level;
      ++result.valuesRead;
    }
  }
  return result;
}

bool DefLevelsAreAllValidNoRepeatedParent(
    const int16_t* def_levels,
    int64_t num_def_levels,
    LevelInfo level_info) {
  bool allValid = true;
  constexpr int64_t kBatch = xsimd::batch<int16_t>::size;
  const auto validLevel = xsimd::broadcast<int16_t>(level_info.def_level - 1);
  int64_t i = 0;
  for (; i + kBatch <= num_def_levels; i += kBatch) {
    const auto levels = xsimd::load_unaligned(def_levels + i);
    allValid &= xsimd::all(levels > validLevel);
  }
  for (; i < num_def_levels; ++i) {
    allValid &= def_levels[i] >= level_info.def_level;
  }
  return allValid;
}

} // namespace

#if defined(ARROW_HAVE_RUNTIME_BMI2)
// defined in level_conversion_bmi2.cc for dynamic dispatch.
void DefLevelsToBitmapBmi2WithRepeatedParent(
    const int16_t* def_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output);
#endif

void DefLevelsToBitmap(
    const int16_t* def_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output) {
  // It is simpler to rely on rep_level here until PARQUET-1899 is done and the
  // code is deleted in a follow-up release.
  if (level_info.rep_level > 0) {
#if defined(ARROW_HAVE_RUNTIME_BMI2)
    if (CpuInfo::GetInstance()->HasEfficientBmi2()) {
      return DefLevelsToBitmapBmi2WithRepeatedParent(
          def_levels, num_def_levels, level_info, output);
    }
#endif
    internal::standard::DefLevelsToBitmapSimd</*has_repeated_parent=*/true>(
        def_levels, num_def_levels, level_info, output);
  } else {
    internal::standard::DefLevelsToBitmapSimd</*has_repeated_parent=*/false>(
        def_levels, num_def_levels, level_info, output);
  }
}

bool DefLevelsAreAllValid(
    const int16_t* def_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    int64_t values_read_upper_bound,
    int64_t* values_read) {
  AllValidResult result;
  if (level_info.rep_level > 0) {
    result = DefLevelsAreAllValidWithRepeatedParent(
        def_levels, num_def_levels, level_info);
  } else {
    result.valuesRead = num_def_levels;
    result.allValid = DefLevelsAreAllValidNoRepeatedParent(
        def_levels, num_def_levels, level_info);
  }
  if (ARROW_PREDICT_FALSE(result.valuesRead > values_read_upper_bound)) {
    throw ParquetException("Values read exceeded upper bound");
  }
  *values_read = result.valuesRead;
  return result.allValid;
}

uint64_t TestOnlyExtractBitsSoftware(uint64_t bitmap, uint64_t select_bitmap) {
  return internal::standard::ExtractBitsSoftware(bitmap, select_bitmap);
}

void DefRepLevelsToList(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output,
    int32_t* offsets) {
  OffsetListOutput<int32_t> listOutput(offsets);
  DefRepLevelsToListInfo(
      def_levels, rep_levels, num_def_levels, level_info, output, listOutput);
}

void DefRepLevelsToList(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output,
    int64_t* offsets) {
  OffsetListOutput<int64_t> listOutput(offsets);
  DefRepLevelsToListInfo(
      def_levels, rep_levels, num_def_levels, level_info, output, listOutput);
}

void DefRepLevelsToListLengths(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output,
    int32_t* lengths) {
  LengthListOutput listOutput(lengths);
  DefRepLevelsToListInfo(
      def_levels, rep_levels, num_def_levels, level_info, output, listOutput);
}

void DefRepLevelsToListLengths(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output,
    int32_t* lengths,
    ListLengthsState* state,
    bool finalize) {
  const auto validityValuesToSkip = static_cast<int64_t>(state->has_open_list);
  LengthListOutput listOutput(lengths, state, finalize);
  DefRepLevelsToListInfo(
      def_levels,
      rep_levels,
      num_def_levels,
      level_info,
      output,
      listOutput,
      validityValuesToSkip,
      finalize);
}

bool DefRepLevelsToListLengthsAndStructBitmap(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo list_level_info,
    LevelInfo struct_level_info,
    ValidityBitmapInputOutput* list_output,
    int32_t* lengths,
    ValidityBitmapInputOutput* struct_output) {
  return defRepLevelsToListLengthsAndStructBitmap(
      def_levels,
      rep_levels,
      num_def_levels,
      list_level_info,
      struct_level_info,
      list_output,
      lengths,
      struct_output,
      nullptr,
      true);
}

bool DefRepLevelsToListLengthsAndStructBitmap(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo list_level_info,
    LevelInfo struct_level_info,
    ValidityBitmapInputOutput* list_output,
    int32_t* lengths,
    ValidityBitmapInputOutput* struct_output,
    ListLengthsState* state,
    bool finalize) {
  return defRepLevelsToListLengthsAndStructBitmap(
      def_levels,
      rep_levels,
      num_def_levels,
      list_level_info,
      struct_level_info,
      list_output,
      lengths,
      struct_output,
      state,
      finalize);
}

void DefRepLevelsToBitmap(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output) {
  // DefRepLevelsToListInfo assumes it for the actual list method and this
  // method is for parent structs, so we need to bump def and ref level.
  level_info.rep_level += 1;
  level_info.def_level += 1;
  OffsetListOutput<int32_t> listOutput(nullptr);
  DefRepLevelsToListInfo(
      def_levels, rep_levels, num_def_levels, level_info, output, listOutput);
}

} // namespace bytedance::bolt::parquet::arrow

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
#include "bolt/dwio/parquet/arrow/Exception.h"

#include "bolt/dwio/parquet/arrow/LevelComparison.h"
#define PARQUET_IMPL_NAMESPACE standard
#include "bolt/dwio/parquet/arrow/LevelConversionInc.h"
#undef PARQUET_IMPL_NAMESPACE
namespace bytedance::bolt::parquet::arrow {
namespace {

using ::arrow::internal::CpuInfo;
using ::std::optional;

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
  explicit LengthListOutput(int32_t* lengths) : lengths_(lengths) {}

  void OnContinuationRun(int64_t run) {
    if (ARROW_PREDICT_FALSE(
            run > std::numeric_limits<int32_t>::max() ||
            currentLength_ > std::numeric_limits<int32_t>::max() - run)) {
      throw ParquetException("List index overflow.");
    }
    currentLength_ += static_cast<int32_t>(run);
  }

  void OnNewList(int16_t def_level, const LevelInfo& level_info) {
    Finish();
    haveCurrentList_ = true;
    currentLength_ = def_level >= level_info.def_level ? 1 : 0;
  }

  void Finish() {
    if (haveCurrentList_) {
      lengths_[valuesRead_++] = currentLength_;
      currentLength_ = 0;
      haveCurrentList_ = false;
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
  int32_t* lengths_;
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
    ListOutput& list_output) {
  optional<::arrow::internal::FirstTimeBitmapWriter> valid_bits_writer;
  if (output->valid_bits) {
    valid_bits_writer.emplace(
        output->valid_bits,
        output->valid_bits_offset,
        output->values_read_upper_bound);
  }
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
      int64_t run = 1;
      while (x + run < num_def_levels &&
             rep_levels[x + run] == level_info.rep_level &&
             def_levels[x + run] >= level_info.repeated_ancestor_def_level) {
        ++run;
      }
      list_output.OnContinuationRun(run);
      x += run - 1;
    } else {
      if (ARROW_PREDICT_FALSE(
              (valid_bits_writer.has_value() &&
               valid_bits_writer->position() >=
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

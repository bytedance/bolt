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
 * 2026-11-19.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 *
 * Tests Parquet level-run generation, encoding, and writer integration.
 */

#include <array>
#include <cstring>
#include <limits>
#include <vector>

#include "arrow/compute/api.h"
#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"
#include "bolt/dwio/parquet/arrow/Metadata.h"
#include "bolt/dwio/parquet/arrow/Writer.h"
#include "bolt/dwio/parquet/arrow/tests/LevelRunTestUtil.h"
#include "parquet/arrow/reader.h"
#include "parquet/page_index.h"

namespace bytedance::bolt::parquet::arrow::test {
namespace {

class LevelRunTest : public ::testing::Test {
 protected:
  ::arrow::ProxyMemoryPool arrow_pool_{::arrow::default_memory_pool()};

  std::shared_ptr<ResizableBuffer> buffer(int64_t size = 0) {
    return AllocateBuffer(&arrow_pool_, size);
  }

  std::shared_ptr<::arrow::Buffer> copy(const int16_t* data, int64_t count) {
    if (!data) {
      return nullptr;
    }
    auto out = buffer(count * sizeof(int16_t));
    if (count != 0) {
      std::memcpy(out->mutable_data(), data, out->size());
    }
    return out;
  }

  void checkHints(LevelRunView runs, const int16_t* levels, int64_t count) {
    int64_t end = 0;
    for (int64_t i = 0; i < runs.size; ++i) {
      const auto& run = runs.data[i];
      ASSERT_GE(run.offset, end);
      ASSERT_GE(run.count, kMinLevelRunLength);
      ASSERT_LE(run.offset + run.count, count);
      for (auto j = run.offset; j < run.offset + run.count; ++j) {
        ASSERT_EQ(run.value, levels[j]) << j;
      }
      if (i && run.offset == end) {
        EXPECT_NE(run.value, runs.data[i - 1].value);
      }
      end = run.offset + run.count;
    }
  }

  void checkReusedGeneration(
      const std::shared_ptr<::arrow::Array>& array,
      bool nullable,
      ArrowWriteContext& reused,
      ArrowWriterProperties* properties) {
    ASSERT_OK_AND_ASSIGN(
        auto builder, arrow::MultipathLevelBuilder::Make(*array, nullable));
    ASSERT_OK_AND_ASSIGN(
        auto fresh_builder,
        arrow::MultipathLevelBuilder::Make(*array, nullable));
    for (int leaf = 0; leaf < builder->GetLeafCount(); ++leaf) {
      ArrowWriteContext fresh(&arrow_pool_, properties);
      ASSERT_OK(builder->Write(leaf, &reused, [&](const auto& actual) {
        return fresh_builder->Write(leaf, &fresh, [&](const auto& expected) {
          EXPECT_EQ(expected.def_rep_level_count, actual.def_rep_level_count);
          const auto compare = [&](const int16_t* lhs, const int16_t* rhs) {
            EXPECT_EQ(lhs == nullptr, rhs == nullptr);
            if (lhs && rhs &&
                expected.def_rep_level_count == actual.def_rep_level_count) {
              EXPECT_EQ(
                  0,
                  std::memcmp(
                      lhs, rhs, actual.def_rep_level_count * sizeof(int16_t)));
            }
          };
          compare(expected.def_levels, actual.def_levels);
          compare(expected.rep_levels, actual.rep_levels);
          const auto* repBuffer = reused.level_scratch
              ? reused.level_scratch->rep_levels_buffer.get()
              : nullptr;
          if (actual.rep_levels) {
            EXPECT_NE(expected.rep_levels, actual.rep_levels);
            EXPECT_NE(nullptr, repBuffer);
            if (repBuffer) {
              EXPECT_EQ(
                  repBuffer->data(),
                  reinterpret_cast<const uint8_t*>(actual.rep_levels));
            }
          } else if (repBuffer) {
            EXPECT_EQ(0, repBuffer->size());
          }
          const auto compare_runs = [&](LevelRunView lhs, LevelRunView rhs) {
            ASSERT_EQ(lhs.size, rhs.size);
            for (int64_t i = 0; i < lhs.size; ++i) {
              EXPECT_EQ(lhs.data[i].offset, rhs.data[i].offset);
              EXPECT_EQ(lhs.data[i].count, rhs.data[i].count);
              EXPECT_EQ(lhs.data[i].value, rhs.data[i].value);
            }
          };
          compare_runs(
              expected.level_runs.definition, actual.level_runs.definition);
          compare_runs(
              expected.level_runs.repetition, actual.level_runs.repetition);
          return ::arrow::Status::OK();
        });
      }));
    }
  }

  void checkGeneration(const std::shared_ptr<::arrow::Array>& array) {
    auto on = ArrowWriterProperties::Builder().build();
    auto off = ArrowWriterProperties::Builder()
                   .set_enable_level_run_optimization(false)
                   ->build();
    ArrowWriteContext enabled(&arrow_pool_, on.get());
    ArrowWriteContext disabled(&arrow_pool_, off.get());
    ASSERT_OK_AND_ASSIGN(
        auto builder, arrow::MultipathLevelBuilder::Make(*array, true));
    for (int leaf = 0; leaf < builder->GetLeafCount(); ++leaf) {
      std::shared_ptr<::arrow::Buffer> def, rep;
      int64_t count = 0;
      ASSERT_OK(builder->Write(leaf, &disabled, [&](const auto& result) {
        count = result.def_rep_level_count;
        def = copy(result.def_levels, count);
        rep = copy(result.rep_levels, count);
        EXPECT_EQ(0, result.level_runs.definition.size);
        EXPECT_EQ(0, result.level_runs.repetition.size);
        return ::arrow::Status::OK();
      }));
      ASSERT_OK(builder->Write(leaf, &enabled, [&](const auto& result) {
        if (count != result.def_rep_level_count) {
          return ::arrow::Status::Invalid(
              "Level counts differ: ",
              count,
              " vs ",
              result.def_rep_level_count);
        }
        if (static_cast<bool>(def) != (result.def_levels != nullptr) ||
            static_cast<bool>(rep) != (result.rep_levels != nullptr)) {
          return ::arrow::Status::Invalid("Level stream presence differs");
        }
        if (count > 0) {
          if (def) {
            EXPECT_EQ(
                0, std::memcmp(def->data(), result.def_levels, def->size()));
          }
          if (rep) {
            EXPECT_EQ(
                0, std::memcmp(rep->data(), result.rep_levels, rep->size()));
          }
        }
        checkHints(result.level_runs.definition, result.def_levels, count);
        checkHints(result.level_runs.repetition, result.rep_levels, count);
        return ::arrow::Status::OK();
      }));
    }
  }
};

TEST_F(LevelRunTest, RepeatedMatchesScalarAcrossCallsAndResets) {
  auto expected = buffer();
  auto actual = buffer();
  IncrementalLevelRleEncoder scalar(expected.get()), repeated(actual.get());
  const std::array<int64_t, 10> lengths{0, 1, 7, 8, 9, 31, 32, 33, 513, 65537};
  for (int16_t max_level : {0, 1, 3, 7, 255, 32767}) {
    for (int prefix : {0, 4}) {
      for (int literal_prefix = 0; literal_prefix < 17; ++literal_prefix) {
        for (auto length : lengths) {
          SCOPED_TRACE(
              ::testing::Message() << max_level << '/' << prefix << '/'
                                   << literal_prefix << '/' << length);
          scalar.Reset(max_level, prefix);
          repeated.Reset(max_level, prefix);
          for (int i = 0; i < literal_prefix; ++i) {
            int16_t value = i % (max_level + 1);
            scalar.Put(&value, 1);
            repeated.Put(&value, 1);
          }
          auto dense = buffer(length * sizeof(int16_t));
          auto levels = reinterpret_cast<int16_t*>(dense->mutable_data());
          std::fill_n(levels, length, max_level);
          scalar.Put(levels, length);
          repeated.PutRepeated(max_level, std::min<int64_t>(7, length));
          repeated.PutRepeated(
              max_level, length - std::min<int64_t>(7, length));
          // Both same-value and different-value transitions back to scalar Put.
          for (int16_t value : {max_level, int16_t{0}, max_level}) {
            scalar.Put(&value, 1);
            repeated.Put(&value, 1);
          }
          ASSERT_EQ(scalar.Flush(), repeated.Flush());
          ASSERT_EQ(
              0,
              std::memcmp(expected->data(), actual->data(), expected->size()));
        }
      }
    }
  }
}

TEST_F(LevelRunTest, ReuseWritableCapacityWithinInt32Limit) {
  // Keep a small physical buffer even when simulating a large aligned capacity.
  class RecordingBuffer : public ResizableBuffer {
   public:
    RecordingBuffer(ResizableBuffer& storage, int64_t capacity)
        : ResizableBuffer(storage.mutable_data(), 0), storage_(storage) {
      capacity_ = capacity;
    }

    ::arrow::Status Resize(int64_t size, bool shrinkToFit) override {
      EXPECT_FALSE(shrinkToFit);
      requests.push_back(size);
      if (size > storage_.size()) {
        return ::arrow::Status::CapacityError("test buffer limit");
      }
      if (size > capacity_) {
        capacity_ = storage_.capacity();
      }
      size_ = size;
      return ::arrow::Status::OK();
    }

    ::arrow::Status Reserve(int64_t) override {
      return ::arrow::Status::NotImplemented("unused");
    }

    std::vector<int64_t> requests;

   private:
    ResizableBuffer& storage_;
  };

  auto storage = buffer(64);
  RecordingBuffer out(*storage, 0);
  IncrementalLevelRleEncoder encoder(&out);
  encoder.Reset(1, 0);
  encoder.PutRepeated(1, 8);
  EXPECT_EQ(2, encoder.Flush());
  EXPECT_EQ((std::vector<int64_t>{0, 1, 64, 2}), out.requests);
  EXPECT_EQ(16, out.data()[0]);
  EXPECT_EQ(1, out.data()[1]);

  out.requests.clear();
  encoder.Reset(1, 4);
  encoder.PutRepeated(1, 8);
  EXPECT_EQ(6, encoder.Flush());
  EXPECT_EQ((std::vector<int64_t>{4, 64, 6}), out.requests);

  constexpr int64_t maxSize = std::numeric_limits<int32_t>::max();
  RecordingBuffer aligned(*storage, maxSize + 1);
  IncrementalLevelRleEncoder large(&aligned);
  large.Reset(1, 0);
  large.PutRepeated(1, 8);
  EXPECT_THROW(large.Flush(), ParquetException);
  EXPECT_EQ((std::vector<int64_t>{0, maxSize}), aligned.requests);
}

TEST_F(LevelRunTest, RejectInvalidAndSplitAtInt32Limit) {
  auto out = buffer();
  IncrementalLevelRleEncoder encoder(out.get());
  encoder.Reset(1, 0);
  EXPECT_NO_THROW(encoder.PutRepeated(-1, 0));
  EXPECT_THROW(encoder.PutRepeated(0, -1), ParquetException);
  EXPECT_THROW(encoder.PutRepeated(-1, 1), ParquetException);
  EXPECT_THROW(encoder.PutRepeated(2, 1), ParquetException);
  EXPECT_THROW(
      encoder.PutRepeated(1, std::numeric_limits<int64_t>::max()),
      ParquetException);
  EXPECT_EQ(0, encoder.Flush());
  for (bool scalar_tail : {false, true}) {
    encoder.Reset(1, 0);
    encoder.PutRepeated(1, std::numeric_limits<int32_t>::max());
    if (scalar_tail) {
      int16_t value = 1;
      encoder.Put(&value, 1);
      encoder.PutRepeated(1, 8);
    } else {
      encoder.PutRepeated(1, 9);
    }
    encoder.Flush();
    const std::array<uint8_t, 8> expected{
        0xfe, 0xff, 0xff, 0xff, 0x0f, 1, 18, 1};
    ASSERT_EQ(expected.size(), out->size());
    EXPECT_EQ(0, std::memcmp(expected.data(), out->data(), expected.size()));
  }
  encoder.Reset(1, 0);
  encoder.PutRepeated(1, int64_t{std::numeric_limits<int32_t>::max()} * 2 + 9);
  EXPECT_EQ(14, encoder.Flush());
}

TEST_F(LevelRunTest, CursorIntersectionsGapsAndPageReset) {
  std::array<int16_t, 1024> levels{};
  std::fill(levels.begin() + 13, levels.begin() + 713, 3);
  std::fill(levels.begin() + 745, levels.end(), 2);
  const std::array<LevelRun, 2> runs{{{13, 700, 3}, {745, 279, 2}}};
  auto expected = buffer(), actual = buffer();
  IncrementalLevelRleEncoder scalar(expected.get()), repeated(actual.get());
  for (int prefix : {0, 4}) {
    LevelRunCursor cursor({runs.data(), runs.size()});
    for (int64_t page = 0; page < 1024; page += 256) {
      scalar.Reset(3, prefix);
      repeated.Reset(3, prefix);
      for (int64_t offset = page; offset < page + 256; offset += 33) {
        auto count = std::min<int64_t>(33, page + 256 - offset);
        scalar.Put(levels.data() + offset, count);
        cursor.Put(repeated, levels.data() + offset, offset, count);
      }
      ASSERT_EQ(scalar.Flush(), repeated.Flush());
      ASSERT_EQ(
          0, std::memcmp(expected->data(), actual->data(), expected->size()));
    }
  }
}

TEST_F(LevelRunTest, GeneratedLevelsSlicesAndNestedParents) {
  for (const auto& pattern :
       {"all_valid",
        "all_null",
        "blocked_null",
        "alternating_null",
        "random_null",
        "long_list",
        "short_list"}) {
    SCOPED_TRACE(pattern);
    auto array = MakeLevelRunInput(pattern, 1536, &arrow_pool_);
    for (int offset : {0, 1, 7, 63, 65}) {
      for (int length : {0, 1, 31, 32, 33, 63, 64, 65, 1025}) {
        checkGeneration(array->Slice(offset, length));
      }
    }
    // Exercise parent BitRunReader and multiple leaf callbacks sharing scratch.
    auto validity = MakeLevelRunInput("blocked_null", 1536, &arrow_pool_);
    auto structure = std::make_shared<::arrow::StructArray>(
        ::arrow::struct_(
            {::arrow::field("a", array->type()),
             ::arrow::field("b", ::arrow::int32())}),
        1536,
        ::arrow::ArrayVector{array, validity},
        validity->null_bitmap(),
        validity->null_count());
    checkGeneration(structure->Slice(7, 1025));
  }
  auto keys = std::make_shared<::arrow::Int32Builder>(&arrow_pool_);
  auto items = std::make_shared<::arrow::Int32Builder>(&arrow_pool_);
  ::arrow::MapBuilder maps(&arrow_pool_, keys, items);
  for (int row = 0; row < 70; ++row) {
    if (row % 5 == 0) {
      ASSERT_OK(maps.AppendNull());
    } else {
      ASSERT_OK(maps.Append());
      for (int i = 0; i < (row % 7 == 0 ? 0 : 96); ++i) {
        ASSERT_OK(keys->Append(i));
        if (i < 40) {
          ASSERT_OK(items->AppendNull());
        } else {
          ASSERT_OK(items->Append(i));
        }
      }
    }
  }
  ASSERT_OK_AND_ASSIGN(auto map, maps.Finish());
  checkGeneration(map->Slice(3, 65));
}

TEST_F(LevelRunTest, MinimumLengthAndMergedBlocks) {
  auto properties = ArrowWriterProperties::Builder().build();
  ArrowWriteContext context(&arrow_pool_, properties.get());
  for (int length : {0, 1, 7, 8, 31, 32, 33, 64, 512}) {
    auto array = MakeLevelRunInput("all_valid", length, &arrow_pool_);
    ASSERT_OK(arrow::MultipathLevelBuilder::Write(
        *array, true, &context, [&](const auto& result) {
          if (result.level_runs.definition.size != (length >= 32 ? 1 : 0) ||
              (length >= 32 && !result.level_runs.definition.data)) {
            return ::arrow::Status::Invalid("Unexpected definition hint count");
          }
          if (length >= 32) {
            EXPECT_EQ(0, result.level_runs.definition.data[0].offset);
            EXPECT_EQ(length, result.level_runs.definition.data[0].count);
            EXPECT_EQ(1, result.level_runs.definition.data[0].value);
          }
          return ::arrow::Status::OK();
        }));
  }
  auto array = MakeLevelRunInput("blocked_null", 512, &arrow_pool_);
  ASSERT_OK(arrow::MultipathLevelBuilder::Write(
      *array, true, &context, [&](const auto& result) {
        if (result.level_runs.definition.size != 2 ||
            !result.level_runs.definition.data) {
          return ::arrow::Status::Invalid("Unexpected blocked-NULL hint count");
        }
        for (int i = 0; i < 2; ++i) {
          EXPECT_EQ(i * 256, result.level_runs.definition.data[i].offset);
          EXPECT_EQ(256, result.level_runs.definition.data[i].count);
          EXPECT_EQ(1 - i, result.level_runs.definition.data[i].value);
        }
        return ::arrow::Status::OK();
      }));
  // Uniform bitmap blocks must fit in the capacity reserved for the leaf.
  EXPECT_EQ(512 * sizeof(int16_t), context.def_levels_buffer->capacity());
  ASSERT_NE(nullptr, context.level_scratch);
  EXPECT_TRUE(context.level_scratch->runs.has_value());
  EXPECT_EQ(nullptr, context.level_scratch->rep_levels_buffer);
}

TEST_F(LevelRunTest, RepetitionBuffersAcrossLeavesAndInputs) {
  auto long_lists = MakeLevelRunInput("long_list", 256, &arrow_pool_);
  auto short_lists = MakeLevelRunInput("short_list", 256, &arrow_pool_);
  auto nullable = MakeLevelRunInput("alternating_null", 256, &arrow_pool_);
  auto required = MakeLevelRunInput("all_valid", 256, &arrow_pool_);
  ::arrow::Int32Builder offsets_builder(&arrow_pool_);
  for (int offset : {0, 64, 64, 128, 256}) {
    ASSERT_OK(offsets_builder.Append(offset));
  }
  ASSERT_OK_AND_ASSIGN(auto offsets, offsets_builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto nested,
      ::arrow::ListArray::FromArrays(*offsets, *short_lists, &arrow_pool_));
  ASSERT_OK_AND_ASSIGN(
      auto maps,
      ::arrow::MapArray::FromArrays(offsets, required, nullable, &arrow_pool_));
  ASSERT_OK_AND_ASSIGN(
      auto structure,
      ::arrow::StructArray::Make(
          {long_lists, nullable, short_lists},
          {::arrow::field("long", long_lists->type()),
           ::arrow::field("flat", nullable->type()),
           ::arrow::field("short", short_lists->type())}));
  const ::arrow::ArrayVector inputs{
      long_lists, short_lists, nested, maps, structure};
  for (bool enabled : {false, true}) {
    auto props = ArrowWriterProperties::Builder()
                     .set_enable_level_run_optimization(enabled)
                     ->build();
    ArrowWriteContext context(&arrow_pool_, props.get());
    checkReusedGeneration(required, false, context, props.get());
    checkReusedGeneration(nullable, true, context, props.get());
    EXPECT_EQ(nullptr, context.level_scratch);
    for (int round = 0; round < 2; ++round) {
      for (const auto& input : inputs) {
        SCOPED_TRACE(input->type()->ToString());
        checkReusedGeneration(input, true, context, props.get());
        checkReusedGeneration(
            input->Slice(1, input->length() - 2), true, context, props.get());
        checkReusedGeneration(input->Slice(0, 0), true, context, props.get());
        checkReusedGeneration(nullable, true, context, props.get());
        checkReusedGeneration(required, false, context, props.get());
        checkReusedGeneration(input, true, context, props.get());
      }
    }
  }
}

TEST_F(LevelRunTest, ScratchAccountedReusedAndClearedAfterError) {
  for (const std::string pattern : {"long_list", "short_list"}) {
    SCOPED_TRACE(pattern);
    const bool longList = pattern == "long_list";
    auto array =
        MakeLevelRunInput(pattern, longList ? 256 : 16384, &arrow_pool_);
    auto small = MakeLevelRunInput("alternating_null", 31, &arrow_pool_);
    auto required = MakeLevelRunInput("all_valid", 31, &arrow_pool_);
    const auto before = arrow_pool_.bytes_allocated();
    for (bool enabled : {false, true}) {
      {
        auto props = ArrowWriterProperties::Builder()
                         .set_enable_level_run_optimization(enabled)
                         ->build();
        ArrowWriteContext context(&arrow_pool_, props.get());
        ASSERT_OK_AND_ASSIGN(
            auto builder, arrow::MultipathLevelBuilder::Make(*array, true));
        const auto callback = [&](const auto& result) {
          EXPECT_NE(nullptr, result.rep_levels);
          const bool expectRuns = enabled && longList;
          EXPECT_EQ(expectRuns, result.level_runs.definition.size > 0);
          EXPECT_EQ(expectRuns, result.level_runs.repetition.size > 0);
          return ::arrow::Status::OK();
        };
        ASSERT_OK(builder->Write(0, &context, callback));
        const auto capacity = context.scratch_allocated_bytes();
        const auto& scratch = context.level_scratch;
        ASSERT_NE(nullptr, scratch);
        ASSERT_NE(nullptr, scratch->rep_levels_buffer);
        EXPECT_EQ(enabled && longList, scratch->runs.has_value());
        const auto rep_capacity = scratch->rep_levels_buffer->capacity();
        const auto* rep_data = scratch->rep_levels_buffer->data();
        const auto allocations = arrow_pool_.num_allocations();
        EXPECT_GT(rep_capacity, 0);
        EXPECT_EQ(capacity, arrow_pool_.bytes_allocated() - before);
        for (int i = 0; i < 20; ++i) {
          ASSERT_OK(builder->Write(0, &context, callback));
          EXPECT_EQ(rep_data, scratch->rep_levels_buffer->data());
          EXPECT_EQ(rep_capacity, scratch->rep_levels_buffer->capacity());
          EXPECT_EQ(capacity, context.scratch_allocated_bytes());
          EXPECT_EQ(before + capacity, arrow_pool_.bytes_allocated());
          EXPECT_EQ(allocations, arrow_pool_.num_allocations());
        }
        EXPECT_FALSE(builder
                         ->Write(
                             0,
                             &context,
                             [](const auto&) {
                               return ::arrow::Status::Invalid(
                                   "injected callback failure");
                             })
                         .ok());
        for (bool nullable : {false, true}) {
          ASSERT_OK(arrow::MultipathLevelBuilder::Write(
              nullable ? *small : *required,
              nullable,
              &context,
              [&](const auto& result) {
                EXPECT_EQ(nullptr, result.rep_levels);
                EXPECT_EQ(0, result.level_runs.definition.size);
                EXPECT_EQ(0, result.level_runs.repetition.size);
                EXPECT_EQ(0, scratch->rep_levels_buffer->size());
                EXPECT_EQ(rep_data, scratch->rep_levels_buffer->data());
                EXPECT_EQ(rep_capacity, scratch->rep_levels_buffer->capacity());
                return ::arrow::Status::OK();
              }));
        }
        ASSERT_OK(builder->Write(0, &context, callback));
        EXPECT_EQ(allocations, arrow_pool_.num_allocations());
      }
      EXPECT_EQ(before, arrow_pool_.bytes_allocated());
    }
  }
}

TEST_F(LevelRunTest, AllocateHintsOnlyForQualifyingLeaves) {
  auto on = ArrowWriterProperties::Builder().build();
  auto off = ArrowWriterProperties::Builder()
                 .set_enable_level_run_optimization(false)
                 ->build();
  ArrowWriteContext enabled(&arrow_pool_, on.get());
  ArrowWriteContext disabled(&arrow_pool_, off.get());
  auto valid = MakeLevelRunInput("all_valid", 257, &arrow_pool_);
  auto mixed = MakeLevelRunInput("alternating_null", 257, &arrow_pool_);
  auto noHints = [](const auto& leaf) {
    EXPECT_EQ(0, leaf.level_runs.definition.size);
    EXPECT_EQ(0, leaf.level_runs.repetition.size);
    return ::arrow::Status::OK();
  };
  ASSERT_OK(
      arrow::MultipathLevelBuilder::Write(*valid, false, &enabled, noHints));
  ASSERT_OK(
      arrow::MultipathLevelBuilder::Write(*mixed, true, &enabled, noHints));
  ASSERT_OK(
      arrow::MultipathLevelBuilder::Write(*valid, true, &disabled, noHints));
  EXPECT_EQ(nullptr, enabled.level_scratch);
  EXPECT_EQ(nullptr, disabled.level_scratch);
  auto shortLists = MakeLevelRunInput("short_list", 1024, &arrow_pool_);
  ASSERT_OK(arrow::MultipathLevelBuilder::Write(
      *shortLists, true, &enabled, noHints));
  ASSERT_NE(nullptr, enabled.level_scratch);
  EXPECT_NE(nullptr, enabled.level_scratch->rep_levels_buffer);
  EXPECT_FALSE(enabled.level_scratch->runs.has_value());
}

TEST_F(LevelRunTest, CompleteFilesIdenticalAndReadable) {
  auto integers =
      MakeLevelRunInput("blocked_null", 4096, &arrow_pool_)->Slice(7, 3073);
  ::arrow::StringBuilder strings(&arrow_pool_);
  for (int i = 0; i < 3073; ++i) {
    if (integers->IsNull(i)) {
      ASSERT_OK(strings.AppendNull());
    } else {
      ASSERT_OK(strings.Append(std::to_string(i)));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto text, strings.Finish());
  ::arrow::compute::ExecContext exec(&arrow_pool_);
  ASSERT_OK_AND_ASSIGN(
      auto dictionary,
      ::arrow::compute::CallFunction("dictionary_encode", {text}, &exec));
  auto lists =
      MakeLevelRunInput("long_list", 3080, &arrow_pool_)->Slice(7, 3073);
  auto shortLists =
      MakeLevelRunInput("short_list", 3076, &arrow_pool_)->Slice(3);
  auto words = std::make_shared<::arrow::StringBuilder>(&arrow_pool_);
  ::arrow::ListBuilder nested(&arrow_pool_, words);
  for (int row = 0; row < 3073; ++row) {
    if (row % 11 == 0) {
      ASSERT_OK(nested.AppendNull());
      continue;
    }
    ASSERT_OK(nested.Append());
    // A single record can exceed the page target. Include empty lists and
    // blocks of null children so value offsets diverge from level offsets.
    if (row % 7 == 0) {
      for (int child = 0; child < 65; ++child) {
        if (child < 32) {
          ASSERT_OK(words->AppendNull());
        } else {
          ASSERT_OK(words->Append(
              std::string(40, 'x') + std::to_string(row + child)));
        }
      }
    }
  }
  ASSERT_OK_AND_ASSIGN(auto nested_strings, nested.Finish());
  for (auto array :
       {MakeLevelRunInput("all_valid", 3073, &arrow_pool_),
        integers,
        std::static_pointer_cast<::arrow::Array>(text),
        dictionary.make_array(),
        lists,
        nested_strings}) {
    const auto& other = array == lists ? shortLists : integers;
    for (auto version :
         {ParquetDataPageVersion::V1, ParquetDataPageVersion::V2}) {
      for (auto compression :
           {Compression::UNCOMPRESSED, Compression::SNAPPY}) {
        for (int dictionary_limit : {0, 128, 1024 * 1024}) {
          SCOPED_TRACE(
              ::testing::Message()
              << array->type()->ToString() << '/' << int(version) << '/'
              << compression << '/' << dictionary_limit);
          std::shared_ptr<::arrow::Buffer> files[2];
          auto schema = ::arrow::schema(
              {::arrow::field("value", array->type()),
               ::arrow::field("other", other->type())});
          for (bool enabled : {false, true}) {
            WriterProperties::Builder properties_builder;
            auto props = properties_builder.memory_pool(&arrow_pool_)
                             ->data_page_version(version)
                             ->compression(compression)
                             ->data_pagesize(1024)
                             ->max_rows_per_page(17)
                             ->write_batch_size(33)
                             ->max_row_group_length(1025)
                             ->dictionary_pagesize_limit(
                                 dictionary_limit ? dictionary_limit : 128);
            const bool writePageIndex = dictionary_limit != 128;
            if (writePageIndex) {
              props->enable_write_page_index();
            }
            if (!dictionary_limit) {
              props->disable_dictionary();
            }
            auto ap = ArrowWriterProperties::Builder()
                          .set_enable_level_run_optimization(enabled)
                          ->set_use_threads(dictionary_limit == 128)
                          ->disable_compliant_nested_types()
                          ->build();
            ASSERT_OK_AND_ASSIGN(
                auto sink,
                ::arrow::io::BufferOutputStream::Create(1024, &arrow_pool_));
            ASSERT_OK_AND_ASSIGN(
                auto writer,
                arrow::FileWriter::Open(
                    *schema, &arrow_pool_, sink, props->build(), ap));
            for (int64_t offset = 0; offset < array->length(); offset += 257) {
              auto slice = array->Slice(offset, 257);
              if (array->type_id() == ::arrow::Type::DICTIONARY &&
                  dictionary_limit == 128 && offset > 0) {
                // Changing the dictionary forces the direct dictionary writer
                // through its dense fallback with the same level coordinates.
                ASSERT_OK_AND_ASSIGN(
                    auto changed_dictionary,
                    ::arrow::compute::CallFunction(
                        "dictionary_encode",
                        {text->Slice(offset, 257)},
                        &exec));
                slice = changed_dictionary.make_array();
              }
              ASSERT_OK(writer->WriteRecordBatch(*::arrow::RecordBatch::Make(
                  schema,
                  slice->length(),
                  {slice, other->Slice(offset, slice->length())})));
            }
            ASSERT_OK(writer->Close());
            EXPECT_EQ(3, writer->metadata()->num_row_groups());
            ASSERT_OK_AND_ASSIGN(files[enabled], sink->Finish());
            auto source =
                std::make_shared<::arrow::io::BufferReader>(files[enabled]);
            std::unique_ptr<::parquet::arrow::FileReader> reader;
            ASSERT_OK(
                ::parquet::arrow::OpenFile(source, &arrow_pool_, &reader));
            if (writePageIndex) {
              auto pageIndex = reader->parquet_reader()->GetPageIndexReader();
              ASSERT_NE(pageIndex, nullptr);
              for (int group = 0; group < writer->metadata()->num_row_groups();
                   ++group) {
                auto groupIndex = pageIndex->RowGroup(group);
                ASSERT_NE(groupIndex, nullptr);
                auto offsets = groupIndex->GetOffsetIndex(0);
                ASSERT_NE(offsets, nullptr);
                int64_t end = writer->metadata()->RowGroup(group)->num_rows();
                const auto& pages = offsets->page_locations();
                for (auto it = pages.rbegin(); it != pages.rend(); ++it) {
                  EXPECT_GT(end - it->first_row_index, 0);
                  EXPECT_LE(end - it->first_row_index, 17);
                  end = it->first_row_index;
                }
                EXPECT_EQ(end, 0);
              }
            }
            std::shared_ptr<::arrow::Table> decoded;
            ASSERT_OK(reader->ReadTable(&decoded));
            // Dictionary arrays are decoded by the independent Arrow reader.
            auto expected = array;
            if (array->type_id() == ::arrow::Type::DICTIONARY) {
              expected = text;
            }
            auto expected_schema = ::arrow::schema(
                {::arrow::field("value", expected->type()),
                 ::arrow::field("other", other->type())});
            auto expected_values = ::arrow::Table::Make(
                expected_schema,
                {std::make_shared<::arrow::ChunkedArray>(expected),
                 std::make_shared<::arrow::ChunkedArray>(other)});
            EXPECT_TRUE(decoded->Equals(*expected_values, false));
            EXPECT_EQ(3073, decoded->num_rows());
            EXPECT_EQ(expected->null_count(), decoded->column(0)->null_count());
          }
          ASSERT_EQ(files[0]->size(), files[1]->size());
          EXPECT_EQ(
              0,
              std::memcmp(
                  files[0]->data(), files[1]->data(), files[0]->size()));
        }
      }
    }
  }
}

} // namespace
} // namespace bytedance::bolt::parquet::arrow::test

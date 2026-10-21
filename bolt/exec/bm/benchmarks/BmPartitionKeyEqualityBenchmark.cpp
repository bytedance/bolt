/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#include <folly/Benchmark.h>

#include <array>

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

constexpr vector_size_t kRows = 64 * 1024;
constexpr vector_size_t kRowsPerPartition = 32;

// This benchmark isolates comparison dispatch and equality work on resident
// rows. It covers short inline keys and 256-byte non-inline keys, but does not
// measure spilled row access.

uint64_t mix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

std::string keyString(uint64_t id, size_t size, uint64_t salt) {
  std::string value(size, 'a');
  auto state = mix64(id + salt);
  for (size_t i = 0; i < size; ++i) {
    if (i != 0 && i % sizeof(state) == 0) {
      state = mix64(state + i);
    }
    value[i] = static_cast<char>('a' + ((state >> ((i % 8) * 8)) & 0xff) % 26);
  }
  return value;
}

void mutateLastCharacter(std::string& value) {
  BOLT_CHECK(!value.empty());
  value.back() = static_cast<char>('a' + (value.back() - 'a' + 1) % 26);
}

enum class EqualityCase {
  kAllEqual,
  kFirstKeyDifferent,
  kMiddleStringDifferent,
  kLongStringEqual,
  kLongStringSamePrefixDifferent,
  kLastKeyDifferent,
  kPeriodicBoundary,
  kNullEqual,
  kNullDifferent,
};

class EqualityFixture {
 public:
  explicit EqualityFixture(EqualityCase testCase)
      : context_("partition-key-equality", kRows * 512),
        types_{BIGINT(), BIGINT(), VARCHAR(), BIGINT(), VARCHAR()},
        nullable_{
            false,
            false,
            false,
            false,
            testCase == EqualityCase::kNullEqual ||
                testCase == EqualityCase::kNullDifferent},
        container_(std::make_unique<BmRowContainer>(
            types_,
            nullable_,
            types_.size(),
            context_.bufferManager,
            memory::bm::MemoryTag::kHashBuild)) {
    test::VectorMaker maker(context_.pool.get());
    const auto entityFor = [testCase](vector_size_t row) -> uint64_t {
      return testCase == EqualityCase::kPeriodicBoundary
          ? row / kRowsPerPartition
          : row / 2;
    };
    const auto firstKeyFor = [testCase, entityFor](vector_size_t row) {
      const auto entity = entityFor(row);
      auto value = testCase == EqualityCase::kPeriodicBoundary
          ? entity
          : mix64(entity + 11) % 1'000'000'000ULL;
      if (testCase == EqualityCase::kFirstKeyDifferent && row % 2 != 0) {
        ++value;
      }
      return static_cast<int64_t>(value);
    };
    const auto middleStringFor = [testCase, entityFor](vector_size_t row) {
      const auto entity = entityFor(row);
      if (testCase == EqualityCase::kLongStringEqual ||
          testCase == EqualityCase::kLongStringSamePrefixDifferent) {
        auto value = keyString(entity, 256, 101);
        if (testCase == EqualityCase::kLongStringSamePrefixDifferent &&
            row % 2 != 0) {
          mutateLastCharacter(value);
        }
        return value;
      }
      auto value = keyString(entity, 3, 101);
      if (testCase == EqualityCase::kMiddleStringDifferent) {
        if (row % 2 != 0) {
          mutateLastCharacter(value);
        }
      }
      return value;
    };
    const auto lastStringFor = [testCase, entityFor](vector_size_t row) {
      auto value = keyString(entityFor(row), 2, 307);
      if (testCase == EqualityCase::kLastKeyDifferent) {
        if (row % 2 != 0) {
          mutateLastCharacter(value);
        }
      }
      return value;
    };
    const auto lastStringNull = [testCase](vector_size_t row) {
      if (testCase == EqualityCase::kNullEqual) {
        return true;
      }
      if (testCase == EqualityCase::kNullDifferent) {
        return row % 2 == 0;
      }
      return false;
    };

    auto input = maker.rowVector({
        maker.flatVector<int64_t>(kRows, firstKeyFor),
        maker.flatVector<int64_t>(
            kRows,
            [entityFor](auto row) {
              return static_cast<int64_t>(mix64(entityFor(row) + 503));
            }),
        maker.flatVector<std::string>(kRows, middleStringFor),
        maker.flatVector<int64_t>(
            kRows,
            [entityFor](auto row) {
              return static_cast<int64_t>(mix64(entityFor(row) + 701));
            }),
        maker.flatVector<std::string>(kRows, lastStringFor, lastStringNull),
    });
    container_->appendBatch(input, kDefaultPartition, &rows_);

    if (testCase == EqualityCase::kPeriodicBoundary) {
      comparisons_.reserve(rows_.size() - 1);
      for (vector_size_t row = 1; row < rows_.size(); ++row) {
        comparisons_.emplace_back(row - 1, row);
      }
    } else {
      comparisons_.reserve(rows_.size() / 2);
      for (vector_size_t row = 0; row + 1 < rows_.size(); row += 2) {
        comparisons_.emplace_back(row, row + 1);
      }
    }

    for (const auto& [left, right] : comparisons_) {
      const auto generic = genericEqual(rows_[left], rows_[right]);
      const auto fast = fastEqual(rows_[left], rows_[right]);
      BOLT_CHECK_EQ(generic, fast);
      expectedEqualCount_ += generic;
    }
  }

  bool genericEqual(const char* left, const char* right) const {
    for (const auto column : keyColumns_) {
      if (container_->compare(left, right, column) != 0) {
        return false;
      }
    }
    return true;
  }

  FOLLY_ALWAYS_INLINE bool fastEqual(const char* left, const char* right)
      const {
    return container_->equalKeys(
        left, right, {keyColumns_.data(), keyColumns_.size()});
  }

  const std::vector<char*>& rows() const {
    return rows_;
  }

  const std::vector<std::pair<vector_size_t, vector_size_t>>& comparisons()
      const {
    return comparisons_;
  }

  uint64_t expectedEqualCount() const {
    return expectedEqualCount_;
  }

 private:
  BenchmarkContext context_;
  std::vector<TypePtr> types_;
  std::vector<bool> nullable_;
  std::unique_ptr<BmRowContainer> container_;
  std::vector<char*> rows_;
  std::vector<std::pair<vector_size_t, vector_size_t>> comparisons_;
  uint64_t expectedEqualCount_{0};
  const std::array<column_index_t, 5> keyColumns_{0, 1, 2, 3, 4};
};

template <bool kFast>
void comparePartitionKeys(uint32_t iterations, EqualityCase testCase) {
  folly::BenchmarkSuspender suspender;
  EqualityFixture fixture(testCase);
  suspender.dismiss();

  uint64_t equalCount = 0;
  const auto& rows = fixture.rows();
  const auto& comparisons = fixture.comparisons();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    for (const auto& [left, right] : comparisons) {
      if constexpr (kFast) {
        equalCount += fixture.fastEqual(rows[left], rows[right]);
      } else {
        equalCount += fixture.genericEqual(rows[left], rows[right]);
      }
    }
  }
  folly::doNotOptimizeAway(equalCount);
  suspender.rehire();
  BOLT_CHECK_EQ(equalCount, fixture.expectedEqualCount() * iterations);
}

void compareGeneric(uint32_t iterations, EqualityCase testCase) {
  comparePartitionKeys<false>(iterations, testCase);
}

void compareFast(uint32_t iterations, EqualityCase testCase) {
  comparePartitionKeys<true>(iterations, testCase);
}

#define REGISTER_EQUALITY_CASE(name, testCase)                        \
  BENCHMARK_NAMED_PARAM(compareGeneric, generic_##name, testCase);    \
  BENCHMARK_RELATIVE_NAMED_PARAM(compareFast, fast_##name, testCase); \
  BENCHMARK_DRAW_LINE()

REGISTER_EQUALITY_CASE(all_equal, EqualityCase::kAllEqual);
REGISTER_EQUALITY_CASE(first_key_different, EqualityCase::kFirstKeyDifferent);
REGISTER_EQUALITY_CASE(
    middle_string_different,
    EqualityCase::kMiddleStringDifferent);
REGISTER_EQUALITY_CASE(long_string_equal, EqualityCase::kLongStringEqual);
REGISTER_EQUALITY_CASE(
    long_string_same_prefix_different,
    EqualityCase::kLongStringSamePrefixDifferent);
REGISTER_EQUALITY_CASE(last_key_different, EqualityCase::kLastKeyDifferent);
REGISTER_EQUALITY_CASE(periodic_boundary, EqualityCase::kPeriodicBoundary);
REGISTER_EQUALITY_CASE(null_equal, EqualityCase::kNullEqual);
REGISTER_EQUALITY_CASE(null_different, EqualityCase::kNullDifferent);

#undef REGISTER_EQUALITY_CASE

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks

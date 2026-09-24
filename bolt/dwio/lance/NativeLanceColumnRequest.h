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

#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>

#include <folly/Range.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/vector/TypeAliases.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceDecodePurpose : uint8_t {
  kFilter,
  kProjection,
  kPrefetch,
};

/// A non-owning, batch-relative row selection. The owner must keep explicit
/// row storage alive until the request has been planned and decoded.
class NativeLanceRowSelection {
 public:
  static NativeLanceRowSelection all() {
    return NativeLanceRowSelection(true, {});
  }

  static NativeLanceRowSelection rows(folly::Range<const vector_size_t*> rows) {
    return NativeLanceRowSelection(false, rows);
  }

  bool selectsAll() const {
    return all_;
  }

  folly::Range<const vector_size_t*> selectedRows() const {
    return rows_;
  }

  vector_size_t outputSize(uint64_t inputRowCount) const {
    BOLT_CHECK_LE(
        inputRowCount,
        static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
    return all_ ? static_cast<vector_size_t>(inputRowCount)
                : static_cast<vector_size_t>(rows_.size());
  }

  void validate(uint64_t inputRowCount) const {
    BOLT_CHECK_LE(
        inputRowCount,
        static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
    if (all_ || rows_.empty()) {
      return;
    }
    BOLT_CHECK_GE(rows_.front(), 0);
    BOLT_CHECK(std::is_sorted(rows_.begin(), rows_.end()));
    BOLT_CHECK(
        std::adjacent_find(rows_.begin(), rows_.end()) == rows_.end(),
        "Selected Lance row numbers must be unique");
    BOLT_CHECK_LT(static_cast<uint64_t>(rows_.back()), inputRowCount);
  }

 private:
  NativeLanceRowSelection(bool all, folly::Range<const vector_size_t*> rows)
      : all_(all), rows_(rows) {}

  bool all_;
  folly::Range<const vector_size_t*> rows_;
};

struct NativeLanceColumnRequest {
  uint64_t rowStart{0};
  uint64_t rowCount{0};
  NativeLanceRowSelection selection{NativeLanceRowSelection::all()};
  NativeLanceDecodePurpose purpose{NativeLanceDecodePurpose::kProjection};

  vector_size_t outputSize() const {
    return selection.outputSize(rowCount);
  }

  void validate() const {
    BOLT_CHECK_LE(rowStart, std::numeric_limits<uint64_t>::max() - rowCount);
    selection.validate(rowCount);
  }
};

} // namespace bytedance::bolt::lance::reader

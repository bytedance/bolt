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

#include <cstdint>
#include <exception>

#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceWindowState : uint8_t {
  kCreated,
  kDecodingFilters,
  kDecodingValues,
  kAssembling,
  kOutputReady,
  kDrained,
  kFailed,
  kCancelled,
};

/// Owns all unpublished data for exactly one RowReader::next() call.
class NativeLanceScanWindow {
 public:
  NativeLanceScanWindow(
      uint64_t generation,
      uint64_t rowStart,
      uint64_t requestedRows);

  void beginDecode(bool hasFilters);
  void filtersReady();
  void beginAssembly();
  void publish(uint64_t scannedRows, VectorPtr result);
  uint64_t drain(VectorPtr& result);
  void fail(std::exception_ptr error);
  void cancel();

  uint64_t generation() const {
    return generation_;
  }

  uint64_t rowStart() const {
    return rowStart_;
  }

  uint64_t requestedRows() const {
    return requestedRows_;
  }

  NativeLanceWindowState state() const {
    return state_;
  }

 private:
  const uint64_t generation_;
  const uint64_t rowStart_;
  const uint64_t requestedRows_;
  uint64_t scannedRows_{0};
  VectorPtr result_;
  std::exception_ptr failure_;
  NativeLanceWindowState state_{NativeLanceWindowState::kCreated};
};

} // namespace bytedance::bolt::lance::reader

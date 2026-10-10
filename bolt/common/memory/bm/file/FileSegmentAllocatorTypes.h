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

namespace bytedance::bolt::memory::bm {

constexpr int64_t kFileSegmentAlignment = 4 * 1024;

enum class FileErrorCode : uint8_t {
  kOk,
  kInvalidConfig,
  kInvalidSize,
  kInvalidSegment,
  kDoubleFree,
  kTooManyOpenFiles,
  kIoError,
  kShutdown,
};

enum class FileSegmentKind : uint8_t {
  kBucket,
  kDedicated,
};

struct FileSegment {
  int fd{-1};
  uint64_t offset{0};
  uint64_t requested_size{0};
  uint64_t allocated_size{0};
  FileSegmentKind kind{FileSegmentKind::kBucket};
  uint64_t id{0};
};

struct FileAllocateResult {
  FileErrorCode error{FileErrorCode::kOk};
  int native_error_code{0};
  FileSegment segment;

  bool ok() const {
    return error == FileErrorCode::kOk;
  }
};

struct FileFreeResult {
  FileErrorCode error{FileErrorCode::kOk};
  int native_error_code{0};

  bool ok() const {
    return error == FileErrorCode::kOk;
  }
};

} // namespace bytedance::bolt::memory::bm

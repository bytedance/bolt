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

#include "bolt/common/memory/bm/io/IoBuffer.h"

namespace bytedance::bolt::memory::bm {

enum class IoErrorCode : uint8_t {
  Ok,
  InvalidRequest,
  Shutdown,
  BackendSubmitFailed,
  BackendIoError,
  ShortIo,
};

struct IoResult {
  IoResult() = default;
  explicit IoResult(
      uint64_t bytes,
      IoErrorCode error = IoErrorCode::Ok,
      int nativeErrorCode = 0)
      : bytes(bytes), error(error), nativeErrorCode(nativeErrorCode) {}

  uint64_t bytes{0};
  IoErrorCode error{IoErrorCode::Ok};
  int nativeErrorCode{0};
  IoBuffer buffer;

  bool ok() const {
    return error == IoErrorCode::Ok;
  }
};

} // namespace bytedance::bolt::memory::bm

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

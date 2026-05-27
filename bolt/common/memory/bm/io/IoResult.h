#pragma once

#include <cstdint>

namespace bytedance::bolt::memory::bm {

enum class IoErrorCode : uint8_t {
  Ok,
  InvalidRequest,
  Shutdown,
  BackendSubmitFailed,
  BackendIoError,
};

struct IoResult {
  uint64_t bytes{0};
  IoErrorCode error{IoErrorCode::Ok};
  int nativeErrorCode{0};

  bool ok() const {
    return error == IoErrorCode::Ok;
  }
};

} // namespace bytedance::bolt::memory::bm

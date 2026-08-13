#pragma once

#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

inline IoErrorCode validateIoRequest(const IoRequest& request) {
  if (!validOpcode(request.opcode) || !validPriority(request.priority)) {
    return IoErrorCode::InvalidRequest;
  }
  if (request.fd < 0 || request.buffer.length() == 0 ||
      !request.buffer.valid()) {
    return IoErrorCode::InvalidRequest;
  }
  return IoErrorCode::Ok;
}

} // namespace bytedance::bolt::memory::bm

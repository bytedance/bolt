#pragma once

#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

inline IoErrorCode validateIoRequest(const IoRequest& request) {
  if (!validOpcode(request.opcode) || !validPriority(request.priority)) {
    return IoErrorCode::InvalidRequest;
  }
  if (request.fd < 0 || !request.buffer.data || request.buffer.length == 0) {
    return IoErrorCode::InvalidRequest;
  }
  if (request.buffer.offset > request.buffer.size) {
    return IoErrorCode::InvalidRequest;
  }
  const auto available = request.buffer.size - request.buffer.offset;
  if (request.buffer.length > available) {
    return IoErrorCode::InvalidRequest;
  }
  return IoErrorCode::Ok;
}

} // namespace bytedance::bolt::memory::bm

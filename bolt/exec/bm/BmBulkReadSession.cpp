#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::exec::bm {

BulkReadSession::BulkReadSession(
    BmRowContainer* container,
    std::vector<SegmentId> segments)
    : container_(container), segments_(std::move(segments)) {}

std::vector<char*> BulkReadSession::loadRows(BulkLoadMetrics* metrics) {
  BOLT_CHECK_NOT_NULL(container_);
  return container_->loadAllRows({segments_.data(), segments_.size()}, metrics);
}

} // namespace bytedance::bolt::exec::bm

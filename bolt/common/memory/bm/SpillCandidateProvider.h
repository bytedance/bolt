#pragma once

#include "bolt/common/memory/bm/BlockHandle.h"

namespace bytedance::bolt::memory::bm {

SpillCandidateProvider MakeBlockHandleSpillCandidateProvider(
    std::span<const std::shared_ptr<BlockHandle>> blocks);

} // namespace bytedance::bolt::memory::bm

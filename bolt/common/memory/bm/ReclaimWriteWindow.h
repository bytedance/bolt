#pragma once

#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>

namespace bytedance::bolt::memory::bm {

class BufferManagerAccounting;
struct BlockMemory;

class ReclaimWriteWindow {
 public:
  using SubmitWrite =
      std::function<SpillWriteFuture(IoBuffer&, size_t, IoPriority)>;

  struct HarvestResult {
    std::shared_ptr<BlockMemory> memory;
    IoResult io;
    uint64_t reclaimedBytes{0};

    bool ok() const {
      return io.ok();
    }
  };

  ReclaimWriteWindow(
      size_t maxInflight,
      IoPriority priority,
      SubmitWrite submitWrite,
      BufferManagerAccounting& accounting);

  bool canSubmit() const;
  bool hasPending() const;
  size_t pendingCount() const;

  void Submit(std::shared_ptr<BlockMemory> memory);
  HarvestResult HarvestNext();

 private:
  struct PendingWrite {
    std::shared_ptr<BlockMemory> memory;
    IoBuffer payload;
    SpillWriteFuture write;
  };

  size_t maxInflight_{0};
  IoPriority priority_;
  SubmitWrite submitWrite_;
  BufferManagerAccounting& accounting_;
  std::deque<PendingWrite> pending_;
};

} // namespace bytedance::bolt::memory::bm

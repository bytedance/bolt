#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/SpillStoreConfig.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/bm/file/ManagedFileSegment.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <future>
#include <memory>

namespace bytedance::bolt::memory::bm {

struct SpillWriteResult {
  IoResult io;
  ManagedFileSegment segment;
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
  uint64_t ioBytes{0};
  uint64_t compressionTimeUs{0};
  bool compressed{false};

  bool ok() const {
    return io.ok();
  }
};

struct SpillWriteMetadata {
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
  uint64_t ioBytes{0};
  uint64_t compressionTimeUs{0};
  bool compressed{false};
};

class SpillWriteFuture {
 public:
  SpillWriteFuture() = default;
  SpillWriteFuture(
      std::future<IoResult> rawFuture,
      ManagedFileSegment segment,
      SpillWriteMetadata metadata);

  SpillWriteResult get();

 private:
  std::future<IoResult> rawFuture_;
  ManagedFileSegment segment_;
  SpillWriteMetadata metadata_;
};

struct SpillReadResult {
  IoResult io;
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
  uint64_t ioBytes{0};
  uint64_t decompressionTimeUs{0};

  bool ok() const {
    return io.ok();
  }
};

class SpillReadFuture {
 public:
  SpillReadFuture() = default;
  SpillReadFuture(
      std::future<IoResult> rawFuture,
      std::shared_ptr<compress::CompressionManager> compression,
      MemoryPool* pool,
      size_t expectedRawSize);
  SpillReadFuture(
      std::future<IoResult> rawFuture,
      std::shared_ptr<compress::CompressionManager> compression,
      MemoryPool* pool,
      size_t recordSize,
      size_t expectedRawSize);

  SpillReadResult get();

 private:
  std::future<IoResult> rawFuture_;
  std::shared_ptr<compress::CompressionManager> compression_;
  MemoryPool* pool_{nullptr};
  size_t recordSize_{0};
  size_t expectedRawSize_{0};
};

class SpillStore {
 public:
  SpillStore(SpillStoreConfig config, MemoryPool* pool);
  ~SpillStore();

  SpillWriteFuture
  SubmitWriteBlock(IoBuffer& payload, size_t rawSize, IoPriority priority);

  SpillReadFuture SubmitReadBlock(
      const ManagedFileSegment& segment,
      size_t expectedRawSize,
      IoPriority priority);

 private:
  FileAllocateResult AllocateSegment(size_t size);
  FileFreeResult FreeSegment(const FileSegment& segment);
  ManagedFileSegment OwnSegment(FileSegment segment) const;

  SpillStoreConfig config_;
  std::shared_ptr<compress::CompressionManager> compression_;
  std::shared_ptr<FileSegmentAllocator> allocator_;
  MemoryPool* pool_{nullptr};
};

} // namespace bytedance::bolt::memory::bm

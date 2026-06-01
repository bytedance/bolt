#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/OwnedFileExtent.h"
#include "bolt/common/memory/bm/SpillStoreConfig.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <future>
#include <memory>

namespace bytedance::bolt::memory::bm {

class SpillCodec;
class SpillIo;

struct SpillWriteResult {
  IoResult io;
  OwnedFileExtent extent;
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
  uint64_t compressionTimeUs{0};
  bool compressed{false};

  bool ok() const {
    return io.ok();
  }
};

struct SpillReadResult {
  IoResult io;
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
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
      MemoryPool* pool,
      compress::CompressionConfig compressionConfig,
      size_t expectedRawSize);

  SpillReadResult get();

 private:
  std::future<IoResult> rawFuture_;
  MemoryPool* pool_{nullptr};
  compress::CompressionConfig compressionConfig_;
  size_t expectedRawSize_{0};
};

class SpillStore {
 public:
  SpillStore(SpillStoreConfig config, MemoryPool* pool);
  ~SpillStore();

  SpillWriteResult
  WriteBlock(IoBuffer& payload, size_t rawSize, IoPriority priority);

  SpillReadFuture SubmitReadBlock(
      const OwnedFileExtent& extent,
      size_t expectedRawSize,
      IoPriority priority);

 private:
  FileAllocateResult AllocateExtent(size_t size);
  FileFreeResult FreeExtent(const FileExtent& extent);
  OwnedFileExtent OwnExtent(FileExtent extent) const;

  SpillStoreConfig config_;
  std::unique_ptr<SpillCodec> codec_;
  std::unique_ptr<SpillIo> io_;
  std::shared_ptr<FileBlockAllocator> allocator_;
  MemoryPool* pool_{nullptr};
};

} // namespace bytedance::bolt::memory::bm

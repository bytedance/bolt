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
  uint64_t compressionTimeUs{0};
  bool compressed{false};

  bool ok() const {
    return io.ok();
  }
};

struct SpillWriteMetadata {
  uint64_t rawBytes{0};
  uint64_t physicalBytes{0};
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

  SpillReadResult get();

 private:
  std::future<IoResult> rawFuture_;
  std::shared_ptr<compress::CompressionManager> compression_;
  MemoryPool* pool_{nullptr};
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

#include "bolt/common/memory/bm/compress/CompressionCodec.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"
#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"
#include "bolt/common/time/Timer.h"

#include <cstring>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace bytedance::bolt::memory::bm::compress {
namespace {

class AlgorithmContextPool {
 public:
  class Ref {
   public:
    Ref() = default;

    Ref(
        std::unique_ptr<CompressionAlgorithmContext> context,
        AlgorithmContextPool* pool)
        : context_(std::move(context)), pool_(pool) {}

    ~Ref() {
      reset();
    }

    Ref(const Ref&) = delete;
    Ref& operator=(const Ref&) = delete;

    Ref(Ref&& other) noexcept
        : context_(std::move(other.context_)), pool_(other.pool_) {
      other.pool_ = nullptr;
    }

    Ref& operator=(Ref&& other) noexcept {
      if (this != &other) {
        reset();
        context_ = std::move(other.context_);
        pool_ = other.pool_;
        other.pool_ = nullptr;
      }
      return *this;
    }

    CompressionAlgorithmContext* get() const {
      return context_.get();
    }

   private:
    void reset() noexcept {
      if (context_ == nullptr) {
        return;
      }
      if (pool_ != nullptr) {
        pool_->Release(std::move(context_));
      } else {
        DestroyCompressionAlgorithmContext(*context_);
      }
    }

    std::unique_ptr<CompressionAlgorithmContext> context_;
    AlgorithmContextPool* pool_{nullptr};
  };

  ~AlgorithmContextPool() {
    for (auto& context : idle_) {
      DestroyCompressionAlgorithmContext(*context);
    }
  }

  Ref Acquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idle_.empty()) {
      return Ref{std::make_unique<CompressionAlgorithmContext>(), this};
    }
    auto context = std::move(idle_.back());
    idle_.pop_back();
    return Ref{std::move(context), this};
  }

 private:
  void Release(std::unique_ptr<CompressionAlgorithmContext> context) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    idle_.push_back(std::move(context));
  }

  std::mutex mutex_;
  std::vector<std::unique_ptr<CompressionAlgorithmContext>> idle_;
};

void writeHeader(
    IoBuffer& record,
    CompressionKind storedKind,
    uint64_t rawSize,
    uint64_t storedSize) {
  SpillRecordHeader header;
  header.compressionKind = static_cast<uint32_t>(storedKind);
  header.rawSize = rawSize;
  header.storedSize = storedSize;
  const auto encoded = EncodeSpillRecordHeader(header);
  std::memcpy(record.data(), encoded.data(), encoded.size());
}

IoBuffer allocateRawPayload(MemoryPool* outputPool, uint64_t rawSize) {
  if (outputPool == nullptr) {
    return IoBuffer::allocateFromMalloc(rawSize);
  }
  return IoBuffer::allocateFromPool(outputPool, rawSize);
}

} // namespace

struct CompressionManager::Impl {
  explicit Impl(CompressionConfig config) : config(std::move(config)) {}

  AlgorithmContextPool lz4Contexts;
  AlgorithmContextPool zstdContexts;
  CompressionConfig config;
};

CompressionManager::CompressionManager(CompressionConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {
  BOLT_CHECK(SupportedCompressionKind(impl_->config.kind));
}

CompressionManager::~CompressionManager() = default;

CompressionRecordResult CompressionManager::BuildSpillRecord(
    std::span<const char> payload) {
  const auto rawSize = static_cast<uint64_t>(payload.size());
  const auto headerSize = sizeof(SpillRecordHeader);

  auto buildUncompressed = [&]() {
    auto record = IoBuffer::allocateFromMalloc(headerSize + payload.size());
    writeHeader(record, CompressionKind::kNone, rawSize, rawSize);
    if (!payload.empty()) {
      std::memcpy(record.data() + headerSize, payload.data(), payload.size());
    }

    CompressionRecordResult result;
    result.record = std::move(record);
    result.rawSize = rawSize;
    result.physicalSize = headerSize + rawSize;
    result.storedKind = CompressionKind::kNone;
    result.compressed = false;
    return result;
  };

  if (impl_->config.kind == CompressionKind::kNone ||
      payload.size() < impl_->config.minCompressBytes || payload.empty()) {
    return buildUncompressed();
  }

  const auto capacity = MaxCompressedLength(impl_->config.kind, payload.size());
  auto record = IoBuffer::allocateFromMalloc(headerSize + capacity);

  auto context =
      impl_->config.kind == CompressionKind::kLz4Block
      ? impl_->lz4Contexts.Acquire()
      : impl_->config.kind == CompressionKind::kZstdFrame
          ? impl_->zstdContexts.Acquire()
          : AlgorithmContextPool::Ref{};

  uint64_t compressionTimeUs = 0;
  uint64_t storedSize = 0;
  {
    MicrosecondTimer timer(&compressionTimeUs);
    storedSize = CompressWithAlgorithm(
        context.get(),
        impl_->config.kind,
        impl_->config,
        payload.data(),
        payload.size(),
        record.data() + headerSize,
        capacity);
  }

  writeHeader(record, impl_->config.kind, rawSize, storedSize);
  record.setLength(headerSize + storedSize);

  CompressionRecordResult result;
  result.record = std::move(record);
  result.rawSize = rawSize;
  result.physicalSize = headerSize + storedSize;
  result.storedKind = impl_->config.kind;
  result.compressionTimeUs = compressionTimeUs;
  result.compressed = true;
  return result;
}

IoBuffer CompressionManager::DecodeSpillRecord(
    std::span<const char> record,
    uint64_t expectedRawSize,
    MemoryPool* outputPool,
    uint64_t* decompressionTimeUs) {
  const auto header =
      DecodeSpillRecordHeader(record.data(), record.size(), expectedRawSize);
  const auto storedKind =
      static_cast<CompressionKind>(header.compressionKind);
  const auto* storedPayload = record.data() + header.headerSize;

  auto rawPayload = allocateRawPayload(outputPool, header.rawSize);
  if (storedKind == CompressionKind::kNone) {
    if (header.storedSize != header.rawSize) {
      BOLT_FAIL(
          "BM uncompressed spill payload size mismatch, stored_size={}, raw_size={}",
          header.storedSize,
          header.rawSize);
    }
    if (header.rawSize > 0) {
      std::memcpy(rawPayload.data(), storedPayload, header.rawSize);
    }
    return rawPayload;
  }

  {
    MicrosecondTimer timer(decompressionTimeUs);
    DecompressWithAlgorithm(
        storedKind,
        storedPayload,
        header.storedSize,
        rawPayload.data(),
        rawPayload.length());
  }
  return rawPayload;
}

} // namespace bytedance::bolt::memory::bm::compress

#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/memory/HashStringAllocator.h"

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include <cstring>

DECLARE_uint64(bm_row_container_data_bytes);
DECLARE_uint64(bm_row_container_warmup_data_bytes);
DEFINE_string(
    bm_row_container_string_store_mode,
    "copy",
    "BM appendBatch string store mode for benchmarks: copy or no_copy. "
    "no_copy is benchmark-only and stores StringViews that reference input "
    "vectors.");

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

template <typename Store>
void copyReusableInputStrings(
    const ReusableInputBatches& input,
    const BenchmarkOptions& options,
    Store store) {
  auto remaining = rowCount(options);
  size_t nextBatch = 0;
  while (remaining > 0) {
    const auto& batch = input.batches[nextBatch];
    const auto batchRows = static_cast<vector_size_t>(
        std::min<uint64_t>(batch->size(), remaining));
    SelectivityVector rows(batchRows);
    DecodedVector decoded(*batch->childAt(3), rows);
    for (vector_size_t row = 0; row < batchRows; ++row) {
      store(decoded.valueAt<StringView>(row));
    }
    remaining -= batchRows;
    nextBatch = (nextBatch + 1) % input.batches.size();
  }
}

BmBatchStringStoreMode stringStoreMode() {
  if (FLAGS_bm_row_container_string_store_mode == "copy") {
    return BmBatchStringStoreMode::kCopy;
  }
  if (FLAGS_bm_row_container_string_store_mode == "no_copy") {
    return BmBatchStringStoreMode::kReferenceInputStringForBenchmark;
  }
  BOLT_FAIL(
      "Unsupported --bm_row_container_string_store_mode={}",
      FLAGS_bm_row_container_string_store_mode);
}

void storeBatchOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupStoreBatchOld(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("store-batch-old", opts.dataBytes);
    auto container = makeOldRowContainer(dataset, context.pool.get());
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();

    storeReusableInputBatchesOldBatch(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

void storeBatchBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    warmupStoreBatchBm(options(dataset, dataBytes(bytes)));
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("store-batch-bm", opts.dataBytes);
    auto container = makeBmRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    const auto mode = stringStoreMode();
    suspender.dismiss();

    storeReusableInputBatchesBmBatch(
        *container, input, opts, nullptr, mode);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

template <bool kUseSimdMemcpy>
void copyStringsToBmHeap(
    BenchmarkContext& context,
    const ReusableInputBatches& input,
    const BenchmarkOptions& opts) {
  constexpr auto kBlockBytes = static_cast<uint32_t>(
      4 * 1024 * 1024);
  std::vector<memory::bm::BufferHandle> blocks;
  blocks.reserve((opts.dataBytes + kBlockBytes - 1) / kBlockBytes);
  char* cursor = nullptr;
  char* limit = nullptr;
  uint64_t copiedBytes = 0;

  copyReusableInputStrings(input, opts, [&](StringView value) {
    if (cursor == nullptr || cursor + value.size() > limit) {
      blocks.push_back(context.bufferManager->Allocate(
          kBlockBytes, memory::bm::MemoryTag::kHashBuild));
      cursor = blocks.back().Ptr();
      limit = cursor + kBlockBytes;
    }
    if constexpr (kUseSimdMemcpy) {
      simd::memcpy(cursor, value.data(), value.size());
    } else {
      std::memcpy(cursor, value.data(), value.size());
    }
    cursor += value.size();
    copiedBytes += value.size();
  });
  folly::doNotOptimizeAway(blocks.size());
  folly::doNotOptimizeAway(copiedBytes);
}

template <bool kUseSimdMemcpy>
void copyStringsToPreallocatedBmHeap(
    const ReusableInputBatches& input,
    const BenchmarkOptions& opts,
    std::vector<memory::bm::BufferHandle>& blocks) {
  constexpr auto kBlockBytes = static_cast<uint32_t>(4 * 1024 * 1024);
  size_t blockIndex = 0;
  char* cursor = blocks[blockIndex].Ptr();
  char* limit = cursor + kBlockBytes;
  uint64_t copiedBytes = 0;

  copyReusableInputStrings(input, opts, [&](StringView value) {
    if (cursor + value.size() > limit) {
      ++blockIndex;
      cursor = blocks[blockIndex].Ptr();
      limit = cursor + kBlockBytes;
    }
    if constexpr (kUseSimdMemcpy) {
      simd::memcpy(cursor, value.data(), value.size());
    } else {
      std::memcpy(cursor, value.data(), value.size());
    }
    cursor += value.size();
    copiedBytes += value.size();
  });
  folly::doNotOptimizeAway(blockIndex);
  folly::doNotOptimizeAway(copiedBytes);
}

void copyStringsToHashStringAllocator(
    BenchmarkContext& context,
    const ReusableInputBatches& input,
    const BenchmarkOptions& opts) {
  HashStringAllocator allocator(context.pool.get());
  StringView stored;
  uint64_t copiedBytes = 0;

  copyReusableInputStrings(input, opts, [&](StringView value) {
    allocator.copyMultipart(
        value, reinterpret_cast<char*>(&stored), /*offset=*/0);
    copiedBytes += value.size();
  });
  folly::doNotOptimizeAway(stored);
  folly::doNotOptimizeAway(copiedBytes);
}

std::vector<memory::bm::BufferHandle> preallocateBmBlocks(
    BenchmarkContext& context,
    const BenchmarkOptions& opts) {
  constexpr auto kBlockBytes = static_cast<uint32_t>(4 * 1024 * 1024);
  std::vector<memory::bm::BufferHandle> blocks;
  blocks.reserve((opts.dataBytes + kBlockBytes - 1) / kBlockBytes);
  auto bytes =
      rowCount(opts) *
      estimatedStringBytesPerRow(opts.dataset, opts.stringProfiles);
  while (bytes > 0) {
    blocks.push_back(context.bufferManager->Allocate(
        kBlockBytes, memory::bm::MemoryTag::kHashBuild));
    bytes -= std::min<uint64_t>(bytes, kBlockBytes);
  }
  return blocks;
}

template <bool kUseSimdMemcpy>
void stringCopyPathBmHeap(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    if (FLAGS_bm_row_container_warmup_data_bytes != 0) {
      auto warmup = options(dataset, FLAGS_bm_row_container_warmup_data_bytes);
      BenchmarkContext context("warmup-string-copy-bm-heap", warmup.dataBytes);
      auto input = makeReusableInputBatches(context.pool.get(), warmup);
      copyStringsToBmHeap<kUseSimdMemcpy>(context, input, warmup);
    }
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("string-copy-bm-heap", opts.dataBytes);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();
    copyStringsToBmHeap<kUseSimdMemcpy>(context, input, opts);
    suspender.rehire();
  }
}

template <bool kUseSimdMemcpy>
void stringCopyPathBmHeapPreallocated(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    if (FLAGS_bm_row_container_warmup_data_bytes != 0) {
      auto warmup = options(dataset, FLAGS_bm_row_container_warmup_data_bytes);
      BenchmarkContext context(
          "warmup-string-copy-bm-heap-preallocated", warmup.dataBytes);
      auto input = makeReusableInputBatches(context.pool.get(), warmup);
      auto blocks = preallocateBmBlocks(context, warmup);
      copyStringsToPreallocatedBmHeap<kUseSimdMemcpy>(input, warmup, blocks);
    }
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("string-copy-bm-heap-preallocated", opts.dataBytes);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    auto blocks = preallocateBmBlocks(context, opts);
    suspender.dismiss();
    copyStringsToPreallocatedBmHeap<kUseSimdMemcpy>(input, opts, blocks);
    suspender.rehire();
  }
}

void strCopyHashAlloc(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  {
    folly::BenchmarkSuspender suspender;
    if (FLAGS_bm_row_container_warmup_data_bytes != 0) {
      auto warmup = options(dataset, FLAGS_bm_row_container_warmup_data_bytes);
      BenchmarkContext context(
          "warmup-string-copy-hash-allocator", warmup.dataBytes);
      auto input = makeReusableInputBatches(context.pool.get(), warmup);
      copyStringsToHashStringAllocator(context, input, warmup);
    }
    suspender.dismiss();
  }
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("string-copy-hash-allocator", opts.dataBytes);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();
    copyStringsToHashStringAllocator(context, input, opts);
    suspender.rehire();
  }
}

void strCopyBmHeapSimd(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  stringCopyPathBmHeap<true>(iterations, dataset, bytes);
}

void strCopyBmHeapStd(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  stringCopyPathBmHeap<false>(iterations, dataset, bytes);
}

void strCopyBmHeapSimdPrealloc(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  stringCopyPathBmHeapPreallocated<true>(iterations, dataset, bytes);
}

BENCHMARK_NAMED_PARAM(storeBatchOld, old_fixed, DatasetKind::kFixed, 0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    storeBatchBm,
    bm_fixed,
    DatasetKind::kFixed,
    0);
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(
    storeBatchOld,
    old_variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    storeBatchBm,
    bm_variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(
    storeBatchOld,
    old_variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    storeBatchBm,
    bm_variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(
    strCopyBmHeapSimd,
    variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    strCopyBmHeapStd,
    variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    strCopyBmHeapSimdPrealloc,
    variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    strCopyHashAlloc,
    variable,
    DatasetKind::kVariable,
    0);
BENCHMARK_NAMED_PARAM(
    strCopyBmHeapSimd,
    variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    strCopyBmHeapStd,
    variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    strCopyBmHeapSimdPrealloc,
    variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_RELATIVE_NAMED_PARAM(
    strCopyHashAlloc,
    variable_large,
    DatasetKind::kVariableLarge,
    0);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks

#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkUtil.h"

#include <fmt/format.h>
#include <folly/Benchmark.h>

namespace bytedance::bolt::exec {

void registerReadMemoryBenchmarks(const std::vector<DatasetSpec>& specs) {
  for (const auto& datasetSpec : specs) {
    const auto* spec = &datasetSpec;
    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_RowContainerReadMemory_1GiB", spec->name),
        [spec]() {
          folly::BenchmarkSuspender suspender;
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format("row-container-readback-benchmark-{}", spec->name),
              benchmarkPoolCapacityBytes(),
              memory::MemoryReclaimer::create());
          auto leaf =
              root->addLeafChild("row-container-readback-benchmark-vectors");
          RowContainer container(spec->keyTypes, spec->dependentTypes, leaf.get());
          std::vector<char*> rows;
          rows.reserve(rowsForTargetBytes(spec->estimatedBytesPerRow));
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            auto batchRows = appendRowContainerBatchReturningRows(container, dataset);
            rows.insert(rows.end(), batchRows.begin(), batchRows.end());
          });
          suspender.dismiss();
          readBackRowContainer(container, rows, allTypes(*spec), leaf.get());
          suspender.rehire();
          return 1;
        });

    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_BmRowContainerReadMemory_1GiB", spec->name),
        [spec]() {
          folly::BenchmarkSuspender suspender;
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format("bm-row-container-readback-benchmark-{}", spec->name),
              benchmarkPoolCapacityBytes(),
              memory::MemoryReclaimer::create());
          auto leaf =
              root->addLeafChild("bm-row-container-readback-benchmark-vectors");
          auto bm = makeBufferManager(
              *root, fmt::format("{}-readback", spec->name));
          BmRowContainer container(spec->keyTypes, spec->dependentTypes, bm);
          std::vector<RowId> rows;
          rows.reserve(rowsForTargetBytes(spec->estimatedBytesPerRow));
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            auto batchRows =
                appendBmRowContainerBatchReturningRows(container, dataset);
            rows.insert(rows.end(), batchRows.begin(), batchRows.end());
          });
          suspender.dismiss();
          readBackBmRowContainer(container, rows, allTypes(*spec), leaf.get());
          const auto stats = bm->stats();
          folly::doNotOptimizeAway(stats.pinCount);
          suspender.rehire();
          printBmStats(
              fmt::format("{}_BmRowContainerReadMemory_1GiB", spec->name),
              stats);
          return 1;
        });
  }
}

} // namespace bytedance::bolt::exec

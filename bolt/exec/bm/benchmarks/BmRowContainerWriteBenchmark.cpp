#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkUtil.h"

#include <fmt/format.h>
#include <folly/Benchmark.h>

namespace bytedance::bolt::exec {

void registerWriteBenchmarks(const std::vector<DatasetSpec>& specs) {
  for (const auto& datasetSpec : specs) {
    const auto* spec = &datasetSpec;
    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_RowContainerWrite_1GiB", spec->name),
        [spec]() {
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format("row-container-benchmark-{}", spec->name),
              kBmRowContainerBenchmarkPoolCapacity,
              memory::MemoryReclaimer::create());
          auto leaf = root->addLeafChild("row-container-benchmark-vectors");
          RowContainer container(spec->keyTypes, spec->dependentTypes, leaf.get());
          uint64_t logicalBytes = 0;
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            appendRowContainerBatch(container, dataset);
            logicalBytes += dataset.logicalBytes;
          });
          folly::doNotOptimizeAway(logicalBytes);
          folly::doNotOptimizeAway(container.numRows());
          return 1;
        });

    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_BmRowContainerWrite_1GiB", spec->name),
        [spec]() {
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format("bm-row-container-benchmark-{}", spec->name),
              kBmRowContainerBenchmarkPoolCapacity,
              memory::MemoryReclaimer::create());
          auto leaf = root->addLeafChild("bm-row-container-benchmark-vectors");
          auto bm = makeBufferManager(*root, spec->name);
          BmRowContainer container(spec->keyTypes, spec->dependentTypes, bm);
          uint64_t logicalBytes = 0;
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            appendBmRowContainerBatch(container, dataset);
            logicalBytes += dataset.logicalBytes;
          });
          folly::doNotOptimizeAway(logicalBytes);
          folly::doNotOptimizeAway(container.numRows());
          return 1;
        });
  }
}

} // namespace bytedance::bolt::exec

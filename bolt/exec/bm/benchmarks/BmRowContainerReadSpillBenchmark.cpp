#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkUtil.h"

#include <fmt/format.h>
#include <folly/Benchmark.h>

namespace bytedance::bolt::exec {

void registerReadSpillBenchmarks(const std::vector<DatasetSpec>& specs) {
  for (const auto& datasetSpec : specs) {
    const auto* spec = &datasetSpec;
    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_RowContainerReadSpill_1GiB", spec->name),
        [spec]() {
          folly::BenchmarkSuspender suspender;
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format("row-container-read-spill-benchmark-{}", spec->name),
              benchmarkPoolCapacityBytes(),
              memory::MemoryReclaimer::create());
          auto leaf =
              root->addLeafChild("row-container-read-spill-benchmark-vectors");
          RowContainer container(spec->keyTypes, spec->dependentTypes, leaf.get());
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            appendRowContainerBatch(container, dataset);
          });
          auto spillConfig = makeRowContainerSpillConfig(
              fmt::format("{}-read-spill", spec->name));
          auto spiller = makeRowContainerSpiller(container, *spec, spillConfig);
          spiller->spill(RowContainerIterator{});
          auto partition = spiller->finishSpill();
          const auto spillStats = spiller->stats();
          suspender.dismiss();
          const auto readStats =
              readRowBasedSpillPartition(partition, container, leaf.get());
          suspender.rehire();
          printRowSpillStats(
              fmt::format("{}_RowContainerReadSpill_1GiB", spec->name),
              spillStats,
              &partition,
              &readStats);
          return 1;
        });

    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_BmRowContainerReadSpill_1GiB", spec->name),
        [spec]() {
          folly::BenchmarkSuspender suspender;
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format(
                  "bm-row-container-spilled-readback-benchmark-{}",
                  spec->name),
              benchmarkPoolCapacityBytes(),
              memory::MemoryReclaimer::create());
          auto leaf = root->addLeafChild(
              "bm-row-container-spilled-readback-benchmark-vectors");
          auto bm = makeBufferManager(
              *root, fmt::format("{}-spilled-readback", spec->name));
          BmRowContainer container(spec->keyTypes, spec->dependentTypes, bm);
          std::vector<RowId> rows;
          rows.reserve(rowsForTargetBytes(spec->estimatedBytesPerRow));
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            auto batchRows =
                appendBmRowContainerBatchReturningRows(container, dataset);
            rows.insert(rows.end(), batchRows.begin(), batchRows.end());
          });
          container.spillAllBlocksForBenchmark();
          const auto spillStats = bm->stats();
          suspender.dismiss();
          readBackBmRowContainer(
              container, rows, spec->keyTypes.front(), leaf.get());
          const auto readStats = bm->stats();
          folly::doNotOptimizeAway(readStats.pinCount);
          suspender.rehire();
          printBmStats(
              fmt::format(
                  "{}_BmRowContainerReadSpill_1GiB.after_spill",
                  spec->name),
              spillStats);
          printBmStats(
              fmt::format(
                  "{}_BmRowContainerReadSpill_1GiB.after_read",
                  spec->name),
              readStats);
          return 1;
        });
  }
}

} // namespace bytedance::bolt::exec

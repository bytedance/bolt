#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkUtil.h"

#include <fmt/format.h>
#include <folly/Benchmark.h>

namespace bytedance::bolt::exec {

void registerSpillBenchmarks(const std::vector<DatasetSpec>& specs) {
  for (const auto& datasetSpec : specs) {
    const auto* spec = &datasetSpec;
    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_RowContainerSpill_1GiB", spec->name),
        [spec]() {
          folly::BenchmarkSuspender suspender;
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format("row-container-spill-benchmark-{}", spec->name),
              kBmRowContainerBenchmarkPoolCapacity,
              memory::MemoryReclaimer::create());
          auto leaf = root->addLeafChild("row-container-spill-benchmark-vectors");
          RowContainer container(spec->keyTypes, spec->dependentTypes, leaf.get());
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            appendRowContainerBatch(container, dataset);
          });
          auto spillConfig =
              makeRowContainerSpillConfig(fmt::format("{}-spill", spec->name));
          auto spiller = makeRowContainerSpiller(container, *spec, spillConfig);
          suspender.dismiss();
          spiller->spill(RowContainerIterator{});
          auto partition = spiller->finishSpill();
          const auto stats = spiller->stats();
          folly::doNotOptimizeAway(partition.size());
          folly::doNotOptimizeAway(partition.rowCount());
          suspender.rehire();
          printRowSpillStats(
              fmt::format("{}_RowContainerSpill_1GiB", spec->name),
              stats,
              &partition);
          return 1;
        });

    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_BmRowContainerSpill_1GiB", spec->name),
        [spec]() {
          folly::BenchmarkSuspender suspender;
          memory::MemoryManager manager;
          auto root = manager.addRootPool(
              fmt::format("bm-row-container-spill-only-benchmark-{}", spec->name),
              kBmRowContainerBenchmarkPoolCapacity,
              memory::MemoryReclaimer::create());
          auto leaf =
              root->addLeafChild("bm-row-container-spill-only-benchmark-vectors");
          auto bm =
              makeBufferManager(*root, fmt::format("{}-spill-only", spec->name));
          BmRowContainer container(spec->keyTypes, spec->dependentTypes, bm);
          forEachBatch(leaf.get(), *spec, [&](const Dataset& dataset, bool) {
            appendBmRowContainerBatch(container, dataset);
          });
          suspender.dismiss();
          container.spillAllBlocksForBenchmark();
          const auto stats = bm->stats();
          folly::doNotOptimizeAway(stats.spillWriteBytes);
          suspender.rehire();
          printBmStats(
              fmt::format("{}_BmRowContainerSpill_1GiB", spec->name), stats);
          return 1;
        });
  }
}

} // namespace bytedance::bolt::exec

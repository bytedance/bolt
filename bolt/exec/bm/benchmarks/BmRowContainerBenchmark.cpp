#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkUtil.h"

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  if (!bytedance::bolt::memory::MemoryManager::testInstance()) {
    bytedance::bolt::memory::MemoryManager::initialize(
        bytedance::bolt::memory::MemoryManager::Options{});
  }
  bytedance::bolt::filesystems::registerLocalFileSystem();

  const auto specs = bytedance::bolt::exec::makeDatasetSpecs();
  bytedance::bolt::exec::registerWriteBenchmarks(specs);
  bytedance::bolt::exec::registerSpillBenchmarks(specs);
  bytedance::bolt::exec::registerReadMemoryBenchmarks(specs);
  bytedance::bolt::exec::registerReadSpillBenchmarks(specs);

  folly::runBenchmarks();
  return 0;
}

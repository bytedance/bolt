#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});
  bytedance::bolt::filesystems::registerLocalFileSystem();
  folly::runBenchmarks();
  return 0;
}

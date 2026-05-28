#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include <memory>
#include <mutex>
#include <utility>

namespace bytedance::bolt::memory::bm {

namespace {

std::mutex gAllocatorMutex;
std::unique_ptr<FileBlockAllocatorImpl> gAllocator;

} // namespace

void initFileBlockAllocator(FileBlockAllocatorConfig config) {
  std::lock_guard<std::mutex> lock(gAllocatorMutex);
  BOLT_CHECK(gAllocator == nullptr, "FileBlockAllocator already initialized");
  gAllocator = std::make_unique<FileBlockAllocatorImpl>(std::move(config));
}

FileBlockAllocator& fileBlockAllocator() {
  std::lock_guard<std::mutex> lock(gAllocatorMutex);
  BOLT_CHECK(gAllocator != nullptr, "FileBlockAllocator is not initialized");
  return *gAllocator;
}

void shutdownFileBlockAllocator() {
  std::lock_guard<std::mutex> lock(gAllocatorMutex);
  gAllocator.reset();
}

} // namespace bytedance::bolt::memory::bm

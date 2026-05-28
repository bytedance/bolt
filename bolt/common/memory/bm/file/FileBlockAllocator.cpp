#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include <memory>
#include <mutex>
#include <utility>

namespace bytedance::bolt::memory::bm {

namespace {

std::mutex g_allocator_mutex;
std::unique_ptr<FileBlockAllocatorImpl> g_allocator;

} // namespace

void InitFileBlockAllocator(FileBlockAllocatorConfig config) {
  std::lock_guard<std::mutex> lock(g_allocator_mutex);
  BOLT_CHECK(g_allocator == nullptr, "FileBlockAllocator already initialized");
  g_allocator = std::make_unique<FileBlockAllocatorImpl>(std::move(config));
}

FileBlockAllocator& GetFileBlockAllocator() {
  std::lock_guard<std::mutex> lock(g_allocator_mutex);
  BOLT_CHECK(g_allocator != nullptr, "FileBlockAllocator is not initialized");
  return *g_allocator;
}

void ShutdownFileBlockAllocator() {
  std::lock_guard<std::mutex> lock(g_allocator_mutex);
  g_allocator.reset();
}

} // namespace bytedance::bolt::memory::bm

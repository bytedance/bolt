/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/connectors/hive/storage_adapters/tos/TosFileSystemExtension.h"

#include <atomic>
#include <mutex>
#include <utility>

namespace bytedance::bolt::filesystems {
namespace {

struct TosFileSystemExtensionRegistry {
  std::mutex mutex;
  std::shared_ptr<const TosFileSystemExtension> owner;
  std::atomic<const TosFileSystemExtension*> extension{nullptr};
};

TosFileSystemExtensionRegistry& extensionRegistry() {
  static auto* registry = new TosFileSystemExtensionRegistry();
  return *registry;
}

const TosFileSystemExtension* getTosFileSystemExtension() {
  return extensionRegistry().extension.load(std::memory_order_acquire);
}

} // namespace

bool registerTosFileSystemExtension(
    std::shared_ptr<const TosFileSystemExtension> extension) {
  if (!extension) {
    return false;
  }

  auto& registry = extensionRegistry();
  std::lock_guard<std::mutex> lock(registry.mutex);
  if (registry.extension.load(std::memory_order_relaxed) != nullptr) {
    return false;
  }
  registry.owner = std::move(extension);
  registry.extension.store(registry.owner.get(), std::memory_order_release);
  return true;
}

void customizeTosClientConfig(VolcengineTos::ClientConfig& config) noexcept {
  try {
    if (const auto* extension = getTosFileSystemExtension()) {
      extension->customizeClientConfig(config);
    }
  } catch (...) {
    // This boundary can be reached before the embedding application has
    // initialized its logging runtime. An optional extension must remain a
    // silent no-op on failure instead of introducing another global-lifecycle
    // dependency here.
  }
}

void notifyTosBucketAccess(
    const TosSessionConfig& sessionConfig,
    const config::ConfigBase& config) noexcept {
  try {
    if (const auto* extension = getTosFileSystemExtension()) {
      extension->notifyBucketAccess(sessionConfig, config);
    }
  } catch (...) {
    // See customizeTosClientConfig(): failure isolation must not depend on
    // glog being initialized.
  }
}

} // namespace bytedance::bolt::filesystems

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

#pragma once

#include <memory>

namespace VolcengineTos {
class ClientConfig;
}

namespace bytedance::bolt {
namespace config {
class ConfigBase;
}

namespace filesystems {

struct TosSessionConfig;

// Optional process-wide hooks for environment-specific TOS behavior. The
// default implementation is a no-op so consumers may override either hook
// independently.
class TosFileSystemExtension {
 public:
  virtual ~TosFileSystemExtension() = default;

  virtual void customizeClientConfig(
      VolcengineTos::ClientConfig& /*config*/) const {}

  // Implementations must be best-effort and non-blocking.
  virtual void notifyBucketAccess(
      const TosSessionConfig& /*sessionConfig*/,
      const config::ConfigBase& /*config*/) const {}
};

// Registers the single process-wide extension. Returns false for a null
// extension or if an extension has already been registered.
bool registerTosFileSystemExtension(
    std::shared_ptr<const TosFileSystemExtension> extension);

// Safe dispatch helpers. Extension failures never affect normal TOS I/O.
void customizeTosClientConfig(VolcengineTos::ClientConfig& config) noexcept;

void notifyTosBucketAccess(
    const TosSessionConfig& sessionConfig,
    const config::ConfigBase& config) noexcept;

} // namespace filesystems
} // namespace bytedance::bolt

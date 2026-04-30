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

#include <chrono>
#include <string>

namespace bytedance::bolt::connector::paimon::test {

// Runs an HDFS+YARN minicluster inside a container for integration tests.
//
// Supported container engines: docker, podman (auto-detected).
//
// Backed by:
//   <container-engine> run ... docker.io/apache/hadoop:3.4.3 mapred minicluster
//
// Environment requirements:
//   - HADOOP_HOME must be set and point to a valid Hadoop installation
//     (e.g., /tmp/hadoop-3.4.3). This is used to locate libhdfs.
//   - The container engine (docker or podman) must be available in PATH.
//
// This class:
// - auto-detects the available container engine (docker or podman)
// - pulls the docker.io/apache/hadoop:3.4.3 image if not present
// - uses a fixed container name for defensive cleanup
// - force-kills any existing container with that name before start
// - ensures the container is removed on Stop() and also via atexit handler
class HdfsContainerMiniCluster {
 public:
  explicit HdfsContainerMiniCluster(std::string containerName);
  ~HdfsContainerMiniCluster();

  void Start(std::chrono::seconds timeout);
  void Stop() noexcept;

  // NameNode RPC endpoint used by libhdfs.
  std::string namenodeUri() const;

 private:
  static void stopAtExit();
  static HdfsContainerMiniCluster* instanceForExit_;

  std::string containerName_;
  std::string hostIp_;
  bool started_{false};
};

} // namespace bytedance::bolt::connector::paimon::test

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

#include "bolt/connectors/paimon/tests/HdfsContainerMiniCluster.h"

#include <fmt/core.h>
#include <glog/logging.h>

#include <cstdlib>
#include <stdexcept>

namespace bytedance::bolt::connector::paimon::test {

namespace {

// NameNode RPC port used by the container-based minicluster.
constexpr int kNameNodeRpcPort = 7878;

std::string quoted(const std::string& s) {
  // Safe quoting for shell invocations in tests.
  std::string out = "'";
  for (char c : s) {
    if (c == '\'') {
      out += "'\\''";
    } else {
      out += c;
    }
  }
  out += "'";
  return out;
}

std::string scriptPathForTests() {
  // Provided by ctest via ENVIRONMENT in CMake.
  const char* dir = ::getenv("PAIMON_TEST_SCRIPT_DIR");
  if (dir != nullptr && dir[0] != '\0') {
    return std::string(dir) + "/hdfs_minicluster.sh";
  }
  // Fallback for direct execution from repo root.
  return "./bolt/connectors/paimon/tests/hdfs_minicluster.sh";
}

int runCmd(const std::string& cmd) {
  return ::system(cmd.c_str());
}

} // namespace

HdfsContainerMiniCluster* HdfsContainerMiniCluster::instanceForExit_{nullptr};

HdfsContainerMiniCluster::HdfsContainerMiniCluster(std::string containerName)
    : containerName_(std::move(containerName)) {}

HdfsContainerMiniCluster::~HdfsContainerMiniCluster() {
  Stop();
}

void HdfsContainerMiniCluster::stopAtExit() {
  if (instanceForExit_) {
    instanceForExit_->Stop();
  }
}

std::string HdfsContainerMiniCluster::namenodeUri() const {
  return fmt::format("hdfs://127.0.0.1:{}", kNameNodeRpcPort);
}

void HdfsContainerMiniCluster::Start(std::chrono::seconds timeout) {
  if (started_) {
    return;
  }

  const std::string scriptPath = scriptPathForTests();
  const int timeoutSeconds = static_cast<int>(timeout.count());
  const std::string cmd = fmt::format(
      "bash {} start --name {} --timeout-seconds {}",
      quoted(scriptPath),
      quoted(containerName_),
      timeoutSeconds);

  LOG(INFO) << "Starting HDFS minicluster via script: " << cmd;
  const int rc = runCmd(cmd);
  if (rc != 0) {
    throw std::runtime_error(fmt::format(
        "Failed to start HDFS minicluster, rc={}, cmd={}", rc, cmd));
  }

  // Ensure cleanup even if test exits early (best-effort; does not cover
  // SIGKILL).
  instanceForExit_ = this;
  static bool registered = false;
  if (!registered) {
    registered = true;
    std::atexit(&HdfsContainerMiniCluster::stopAtExit);
  }

  started_ = true;
}

void HdfsContainerMiniCluster::Stop() noexcept {
  // Clear the atexit pointer to avoid re-entrancy during process shutdown.
  if (instanceForExit_ == this) {
    instanceForExit_ = nullptr;
  }

  const std::string scriptPath = scriptPathForTests();
  const std::string cmd = fmt::format(
      "bash {} stop --name {}", quoted(scriptPath), quoted(containerName_));
  runCmd(cmd);
  if (!started_) {
    return;
  }
  started_ = false;
}

} // namespace bytedance::bolt::connector::paimon::test

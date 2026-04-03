/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/connectors/paimon/tests/HdfsContainerMiniCluster.h"

#include <fmt/core.h>
#include <glog/logging.h>
#include <cstdlib>
#include <stdexcept>
#include <thread>

#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>

namespace bytedance::bolt::connector::paimon::test {

namespace {

// Expose commonly-used NameNode + DataNode ports.
//
// NameNode:
//  - 7878: RPC
//  - 7676: HTTP
constexpr int kNameNodeRpcPort = 7878;
constexpr int kNameNodeHttpPort = 7676;

constexpr const char* kHadoopImage = "docker.io/apache/hadoop:3.4.3";

int runCmd(const std::string& cmd) {
  return ::system(cmd.c_str());
}

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

// Detects and returns the available container engine ("docker" or "podman").
// Throws std::runtime_error if neither is available.
std::string detectContainerEngine() {
  // Check for docker first, then podman.
  if (runCmd("docker --version >/dev/null 2>&1") == 0) {
    return "docker";
  }
  if (runCmd("podman --version >/dev/null 2>&1") == 0) {
    return "podman";
  }
  throw std::runtime_error(
      "Neither docker nor podman is available. Please install one of them.");
}

// Checks if the container image exists locally (via `docker/podman image
// inspect`). Returns true if the image is present, false otherwise.
bool imageExistsLocally(const std::string& engine) {
  return runCmd(fmt::format(
             "{} image inspect {} >/dev/null 2>&1", engine, kHadoopImage)) == 0;
}

// Pulls the container image if it does not exist locally.
void ensureImagePulled(const std::string& engine) {
  if (imageExistsLocally(engine)) {
    LOG(INFO) << fmt::format(
        "Image {} already present locally, skipping pull", kHadoopImage);
    return;
  }
  LOG(INFO) << fmt::format(
      "Pulling image {} (this may take a while)...", kHadoopImage);
  const int rc = runCmd(fmt::format("{} pull {}", engine, kHadoopImage));
  if (rc != 0) {
    throw std::runtime_error(
        fmt::format("Failed to pull image {}, rc={}", kHadoopImage, rc));
  }
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

  // Detect and validate container engine.
  const std::string engine = detectContainerEngine();
  LOG(INFO) << "Using container engine: " << engine;

  // Ensure image is present before starting the container.
  ensureImagePulled(engine);

  // Defensive cleanup of any existing container with the same name.
  runCmd(fmt::format(
      "{} rm -f {} >/dev/null 2>&1", engine, quoted(containerName_)));

  // Run the Hadoop minicluster in detached mode.
  // For tests we use -d (detach) and a fixed --name.
  auto startWithCommand = [&](const std::string& runCmdString) {
    const int rc = runCmd(runCmdString);
    if (rc != 0) {
      throw std::runtime_error(fmt::format(
          "Failed to start HDFS container, rc={}, cmd={}", rc, runCmdString));
    }

    // Ensure cleanup even if test exits early (best-effort; does not cover
    // SIGKILL).
    instanceForExit_ = this;
    static bool registered = false;
    if (!registered) {
      registered = true;
      std::atexit(&HdfsContainerMiniCluster::stopAtExit);
    }

    // Wait for readiness by executing a simple HDFS command inside the
    // container. This avoids relying on host tools.
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    const std::string check = fmt::format(
        "{} exec {} hdfs dfs -ls hdfs://localhost:7878/ >/dev/null 2>&1",
        engine,
        quoted(containerName_));

    while (std::chrono::steady_clock::now() < deadline) {
      if (runCmd(check) == 0) {
        LOG(INFO) << "Passed dfs -ls health check";
        runCmd(fmt::format(
            "{} logs {} | grep -E 'RPC server is binding|NameNode RPC up' | tail -n 5 >&2 || true",
            engine,
            quoted(containerName_)));
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }

    runCmd(
        fmt::format("{} logs {} >&2 || true", engine, quoted(containerName_)));
    throw std::runtime_error(
        "Timed out waiting for HDFS minicluster readiness");
  };

  // Primary mode: host networking.
  const std::string runHostNet = fmt::format(
      "{} run --rm -d --name {} --network host "
      "{} mapred minicluster "
      "-format -nomr "
      "-nnhttpport {} -nnport {} "
      "-D dfs.permissions=false ",
      engine,
      quoted(containerName_),
      kHadoopImage,
      kNameNodeHttpPort,
      kNameNodeRpcPort);

  LOG(INFO) << runHostNet;

  startWithCommand(runHostNet);

  started_ = true;
}

void HdfsContainerMiniCluster::Stop() noexcept {
  // Clear the atexit pointer to avoid re-entrancy during process shutdown.
  if (instanceForExit_ == this) {
    instanceForExit_ = nullptr;
  }

  // Detect engine for cleanup (best effort; engine may already be unavailable).
  std::string engine;
  try {
    engine = detectContainerEngine();
  } catch (...) {
    // Engine unavailable, skip logging and removal.
    return;
  }
  runCmd(fmt::format(
      "{} rm -f {} >/dev/null 2>&1", engine, quoted(containerName_)));
  if (!started_) {
    return;
  }
  started_ = false;
}

} // namespace bytedance::bolt::connector::paimon::test

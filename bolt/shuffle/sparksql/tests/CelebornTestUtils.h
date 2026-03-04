/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <celeborn/client/ShuffleClient.h>
#include <celeborn/conf/CelebornConf.h>
#include <folly/Conv.h>
#include <folly/init/Init.h>
#include <atomic>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <unistd.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql::test {

inline constexpr const char* kRunE2EEnv = "BOLT_CELEBORN_E2E";
inline constexpr const char* kRealCelebornEnv =
    "BOLT_SHUFFLE_TEST_REAL_CELEBORN";
inline constexpr const char* kCelebornLmEndpointFileEnv =
    "BOLT_CELEBORN_LM_ENDPOINT_FILE";
inline constexpr const char* kCelebornLmAppIdEnv = "BOLT_CELEBORN_LM_APP_ID";
inline constexpr std::string_view kDefaultLmEndpointFile =
    "bolt/shuffle/sparksql/tests/celeborn/runtime/state/lifecycle_manager.endpoint";

inline bool readBoolEnv(const char* env, bool defaultValue = false) {
  const char* value = std::getenv(env);
  if (value == nullptr || std::strlen(value) == 0) {
    return defaultValue;
  }
  return folly::tryTo<bool>(value).value_or(defaultValue);
}

inline std::string getEnvOrDefault(
    const char* env,
    std::string_view defaultValue) {
  const char* value = std::getenv(env);
  if (value == nullptr || std::strlen(value) == 0) {
    return std::string(defaultValue);
  }
  return std::string(value);
}

inline void ensureFollyInitializedForCeleborn() {
  static std::once_flag once;
  std::call_once(once, [] {
    int argc = 1;
    char arg0[] = "bolt-celeborn-test";
    char* argv[] = {arg0, nullptr};
    char** argvPtr = argv;
    folly::init(&argc, &argvPtr, false);
  });
}

inline std::pair<std::string, int> loadLifecycleManagerEndpointForTests(
    std::string_view defaultEndpointFile = kDefaultLmEndpointFile) {
  const auto endpointFile =
      getEnvOrDefault(kCelebornLmEndpointFileEnv, defaultEndpointFile);

  std::ifstream in(endpointFile);
  BOLT_CHECK(
      in.good(),
      "Cannot open lifecycle manager endpoint file: " + endpointFile);

  std::string endpoint;
  std::getline(in, endpoint);

  const auto pos = endpoint.rfind(':');
  BOLT_CHECK(
      pos != std::string::npos,
      "Invalid lifecycle manager endpoint: " + endpoint);

  return {endpoint.substr(0, pos), std::stoi(endpoint.substr(pos + 1))};
}

inline std::shared_ptr<celeborn::client::ShuffleClient>
createRealCelebornClientForTests(const std::string& appId) {
  auto [lmHost, lmPort] = loadLifecycleManagerEndpointForTests();
  ensureFollyInitializedForCeleborn();
  auto conf = std::make_shared<celeborn::conf::CelebornConf>();
  auto endpoint = celeborn::client::ShuffleClientEndpoint(conf);
  auto client =
      celeborn::client::ShuffleClientImpl::create(appId, conf, endpoint);
  client->setupLifecycleManagerRef(lmHost, lmPort);
  return client;
}

// Generates a process-scoped shuffle id range to reduce collisions across
// concurrently running test processes.
inline int nextCelebornShuffleIdForTests() {
  constexpr int kShuffleIdStride = 10000;
  constexpr int kPidModulo = 50000;
  constexpr int kShuffleIdBase = 10000;
  const int processId = static_cast<int>(getpid());
  const int pidBucket = std::abs(processId) % kPidModulo;
  static std::atomic<int> nextShuffleId{
      kShuffleIdBase + pidBucket * kShuffleIdStride};
  return nextShuffleId.fetch_add(1, std::memory_order_relaxed);
}

class RealCelebornClientCleanupGuard {
 public:
  explicit RealCelebornClientCleanupGuard(
      std::shared_ptr<celeborn::client::ShuffleClient>* client)
      : client_(client) {}

  RealCelebornClientCleanupGuard(const RealCelebornClientCleanupGuard&) =
      delete;
  RealCelebornClientCleanupGuard& operator=(
      const RealCelebornClientCleanupGuard&) = delete;

  // Lazily allocates a shuffle id used by cleanupShuffle.
  int shuffleId() {
    if (!shuffleId_.has_value()) {
      shuffleId_ = nextCelebornShuffleIdForTests();
    }
    return *shuffleId_;
  }

  // Allows tests to pin a specific shuffle id when needed.
  void setShuffleId(int shuffleId) {
    shuffleId_ = shuffleId;
  }

  // Runs cleanup immediately and propagates the first failure, while still
  // attempting shutdown/reset. This is used on normal paths so failures are
  // visible to tests.
  void cleanupNow() {
    if (client_ == nullptr || *client_ == nullptr || finalized_) {
      return;
    }

    std::exception_ptr firstError;
    if (shuffleId_.has_value() && !shuffleCleaned_) {
      try {
        (*client_)->cleanupShuffle(*shuffleId_);
        shuffleCleaned_ = true;
      } catch (...) {
        firstError = std::current_exception();
      }
    }

    try {
      (*client_)->shutdown();
    } catch (...) {
      if (firstError == nullptr) {
        firstError = std::current_exception();
      }
    }

    client_->reset();
    finalized_ = true;

    if (firstError != nullptr) {
      std::rethrow_exception(firstError);
    }
  }

  // Best-effort cleanup.  Never throw from destructor because tests may be
  // unwinding due to prior failures.
  ~RealCelebornClientCleanupGuard() {
    try {
      cleanupNow();
    } catch (...) {
    }
  }

 private:
  std::shared_ptr<celeborn::client::ShuffleClient>* client_{nullptr};
  std::optional<int> shuffleId_;
  bool shuffleCleaned_{false};
  bool finalized_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql::test

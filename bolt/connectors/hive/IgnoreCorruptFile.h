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

#include <fmt/core.h>
#include <folly/Format.h>
#include <glog/logging.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/StorageException.h"

class IgnoreCorruptFileHelper {
 public:
  static void globalInitialize(
      int64_t taskMaxFailures,
      bool enableIgnoreCorruptFiles,
      const std::string& userDefineIgnoreExceptions) {
    bool inTestEnv = isInTestEnv();
    if (hasInitialized_ && !inTestEnv) {
      return;
    }
    taskMaxFailures_ = taskMaxFailures;
    enableIgnoreCorruptFiles_ = enableIgnoreCorruptFiles;
    {
      std::unique_lock guard(mutex_);
      enrichExceptionSetFromConf(
          userDefineIgnoreExceptions, userDefinedSet_, inTestEnv);
    }
    hasInitialized_ = true;
  }
  static bool isIgnoreCorruptFilesEnabled() {
    BOLT_CHECK(
        hasInitialized_,
        "IgnoreCorruptFileHelper is not initialized, please call initialize() first");
    return enableIgnoreCorruptFiles_;
  }
  static bool isLastRetry(const std::string& taskId) {
    BOLT_CHECK(
        hasInitialized_,
        "IgnoreCorruptFileHelper is not initialized, please call initialize() first");
    // ignoreCorruptFiles_ only set in gluten/spark env, so it's safe to
    // extract attempt number from task id
    const auto attemptNumber = extractAttemptNumber(taskId);
    BOLT_CHECK(
        attemptNumber != -1,
        "Failed to extract attempt number from task ID: {}",
        taskId);
    return attemptNumber >= taskMaxFailures_ - 1;
  }
  static bool canBeIgnoredInMustIgnoreSet(
      ::bytedance::bolt::filesystems::StorageException::StorageErrorType
          errorType) {
    BOLT_CHECK(
        hasInitialized_,
        "IgnoreCorruptFileHelper is not initialized, please call initialize() first");
    return IgnoreCorruptFileHelper::kMustIgnoredExceptions.find(errorType) !=
        IgnoreCorruptFileHelper::kMustIgnoredExceptions.end();
  }
  static bool canBeIgnoredInUserDefineSet(const std::string& msg) {
    BOLT_CHECK(
        hasInitialized_,
        "IgnoreCorruptFileHelper is not initialized, please call initialize() first");
    for (const std::string& expected : userDefinedSet_) {
      if (msg.find(expected) != std::string::npos) {
        return true;
      }
    }
    return false;
  }

 private:
  static bool isInTestEnv() {
    return std::getenv("BOLT_IN_GTEST") != nullptr;
  }

  static void enrichExceptionSetFromConf(
      std::string exceptionStr,
      std::vector<std::string>& exceptionKeyWords,
      bool overrideSet) {
    // only init exceptionSet exactly once
    if (exceptionStr.empty()) {
      return;
    }
    if (!exceptionKeyWords.empty() && !overrideSet) {
      return;
    }
    exceptionKeyWords.clear();
    const char delimiter = exceptionStr.back();
    exceptionStr.pop_back();
    folly::split(delimiter, exceptionStr, exceptionKeyWords);
    LOG(INFO) << "Split exception str by " << (char)delimiter
              << ", and split result is: "
              << fmt::format("{}", fmt::join(exceptionKeyWords, ", "));
  }
  static int64_t extractAttemptNumber(const std::string& taskId) {
    const std::string prefix = "ATTEMPT_";
    size_t prefix_pos = taskId.find(prefix);
    if (prefix_pos == std::string::npos) {
      return -1;
    }
    size_t num_start = prefix_pos + prefix.length();
    size_t num_end = num_start;
    while (num_end < taskId.length() && std::isdigit(taskId[num_end])) {
      num_end++;
    }
    if (num_end == num_start) {
      return -1;
    }
    try {
      return std::stoi(taskId.substr(num_start, num_end - num_start));
    } catch (...) {
      return -1;
    }
    return -1;
  }
  inline static bool hasInitialized_{false};
  inline static int64_t taskMaxFailures_{0};
  inline static bool enableIgnoreCorruptFiles_{false};
  inline static std::mutex mutex_;
  inline static std::vector<std::string> userDefinedSet_{};
  inline const static std::set<
      ::bytedance::bolt::filesystems::StorageException::StorageErrorType>
      kMustIgnoredExceptions{
          ::bytedance::bolt::filesystems::StorageException::StorageErrorType::
              HdfsIOException,
          ::bytedance::bolt::filesystems::StorageException::StorageErrorType::
              MissingBlockException};
};

#ifndef TRY_WITH_IGNORE
#define TRY_WITH_IGNORE(taskId, operation, onIgnoreOperation)                  \
  do {                                                                         \
    if (!IgnoreCorruptFileHelper::isIgnoreCorruptFilesEnabled()) {             \
      operation;                                                               \
    } else {                                                                   \
      try {                                                                    \
        operation;                                                             \
      } catch (const ::bytedance::bolt::filesystems::StorageException& e) {    \
        bool canbeIgnored = (IgnoreCorruptFileHelper::isLastRetry(taskId)) &&  \
            IgnoreCorruptFileHelper::canBeIgnoredInMustIgnoreSet(              \
                                e.getStorageErrorType());                      \
        LOG(ERROR) << "Catch StorageException, exception_type="                \
                   << e.getStorageErrorType()                                  \
                   << (canbeIgnored ? "Can" : "Can't")                         \
                   << " be ignored, exception_msg="                            \
                   << e.getStorageErrorMessage();                              \
        if (canbeIgnored) {                                                    \
          onIgnoreOperation;                                                   \
        } else {                                                               \
          std::rethrow_exception(std::current_exception());                    \
        }                                                                      \
      } catch (                                                                \
          const dwio::common::exception::DecompressionLoggedException& e) {    \
        bool canbeIgnored = (IgnoreCorruptFileHelper::isLastRetry(taskId));    \
        LOG(ERROR) << "Catch DecompressionLoggedException. "                   \
                   << (canbeIgnored ? "Can" : "Can't")                         \
                   << " be ignored, exception_msg=" << e.what();               \
        if (canbeIgnored) {                                                    \
          onIgnoreOperation;                                                   \
        } else {                                                               \
          std::rethrow_exception(std::current_exception());                    \
        }                                                                      \
      } catch (const std::exception& e) {                                      \
        std::string errorReason = e.what();                                    \
        bool isLastRetry = IgnoreCorruptFileHelper::isLastRetry(taskId);       \
        bool inIgnoreSet =                                                     \
            IgnoreCorruptFileHelper::canBeIgnoredInUserDefineSet(errorReason); \
        bool canbeIgnored = isLastRetry && inIgnoreSet;                        \
        LOG(ERROR) << "Catch std::exception. "                                 \
                   << (canbeIgnored ? "Can" : "Can't")                         \
                   << " be ignored, isLastRetry is "                           \
                   << (isLastRetry ? "true" : "false") << ", inIgnoreSet is "  \
                   << (inIgnoreSet ? "true" : "false")                         \
                   << " exception_msg=" << e.what();                           \
        if (canbeIgnored) {                                                    \
          onIgnoreOperation;                                                   \
        } else {                                                               \
          std::rethrow_exception(std::current_exception());                    \
        }                                                                      \
      }                                                                        \
    }                                                                          \
  } while (false);
#endif

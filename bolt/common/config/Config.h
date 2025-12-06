/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <sys/utsname.h>
#include <functional>
#include <map>
#include <shared_mutex>
#include <unordered_map>

#include "bolt/common/base/Exceptions.h"
#include "folly/Conv.h"
namespace bytedance::bolt::config {

enum class CapacityUnit {
  BYTE,
  KILOBYTE,
  MEGABYTE,
  GIGABYTE,
  TERABYTE,
  PETABYTE
};

double toBytesPerCapacityUnit(CapacityUnit unit);

CapacityUnit valueOfCapacityUnit(const std::string& unitStr);

/// Convert capacity string with unit to the capacity number in the specified
/// units
uint64_t toCapacity(const std::string& from, CapacityUnit to);

std::chrono::duration<double> toDuration(const std::string& str);

/// The concrete config class should inherit the config base and define all the
/// entries.
class ConfigBase {
 public:
  template <typename T>
  struct Entry {
    Entry(
        const std::string& _key,
        const T& _val,
        std::function<std::string(const T&)> _toStr =
            [](const T& val) { return folly::to<std::string>(val); },
        std::function<T(const std::string&, const std::string&)> _toT =
            [](const std::string& k, const std::string& v) {
              auto converted = folly::tryTo<T>(v);
              BOLT_CHECK(
                  converted.hasValue(),
                  fmt::format(
                      "Invalid configuration for key '{}'. Value '{}' cannot be converted to type {}.",
                      k,
                      v,
                      folly::demangle(typeid(T))));
              return converted.value();
            })
        : key{_key}, defaultVal{_val}, toStr{_toStr}, toT{_toT} {}

    const std::string key;
    const T defaultVal;
    const std::function<std::string(const T&)> toStr;
    const std::function<T(const std::string&, const std::string&)> toT;
  };

  ConfigBase(
      std::unordered_map<std::string, std::string>&& configs,
      bool _mutable = false)
      : configs_(std::move(configs)), mutable_(_mutable) {}

  virtual ~ConfigBase() {}

  template <typename T>
  ConfigBase& set(const Entry<T>& entry, const T& val) {
    BOLT_CHECK(mutable_, "Cannot set in immutable config");
    std::unique_lock<std::shared_mutex> l(mutex_);
    configs_[entry.key] = entry.toStr(val);
    return *this;
  }

  ConfigBase& set(const std::string& key, const std::string& val);

  template <typename T>
  ConfigBase& unset(const Entry<T>& entry) {
    BOLT_CHECK(mutable_, "Cannot unset in immutable config");
    std::unique_lock<std::shared_mutex> l(mutex_);
    configs_.erase(entry.key);
    return *this;
  }

  ConfigBase& reset();

  template <typename T>
  T get(const Entry<T>& entry) const {
    std::shared_lock<std::shared_mutex> l(mutex_);
    auto iter = configs_.find(entry.key);
    return iter != configs_.end() ? entry.toT(entry.key, iter->second)
                                  : entry.defaultVal;
  }

  template <typename T>
  folly::Optional<T> get(
      const std::string& key,
      std::function<T(std::string, std::string)> toT = [](auto /* unused */,
                                                          auto value) {
        return folly::to<T>(value);
      }) const {
    auto val = get(key);
    if (val.hasValue()) {
      return toT(key, val.value());
    } else {
      return folly::none;
    }
  }

  template <typename T>
  T get(
      const std::string& key,
      const T& defaultValue,
      std::function<T(std::string, std::string)> toT = [](auto /* unused */,
                                                          auto value) {
        return folly::to<T>(value);
      }) const {
    auto val = get(key);
    if (val.hasValue()) {
      return toT(key, val.value());
    } else {
      return defaultValue;
    }
  }

  bool valueExists(const std::string& key) const;

  const std::unordered_map<std::string, std::string>& rawConfigs() const;

  std::unordered_map<std::string, std::string> rawConfigsCopy() const;

 protected:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::string> configs_;

 private:
  folly::Optional<std::string> get(const std::string& key) const;

  const bool mutable_;
};

/*
 * For abtesting
 */
enum AB_MODE { OFF, ON, ABTEST };

FOLLY_ALWAYS_INLINE AB_MODE STR_TO_AB_MODE(const std::string& input) {
  if (input == "true")
    return ON;
  else if (input == "false")
    return OFF;
  else if (input == "abtest")
    return ABTEST;
  else {
    LOG(ERROR) << "Unknown status: " << input << ". Default to false";
    return OFF;
  }
}

FOLLY_ALWAYS_INLINE bool CHECK_SWITCH_IN_ABTEST(
    AB_MODE switchValue,
    bool isControlGroup) {
  return (switchValue == ABTEST) ? isControlGroup
      : (switchValue == ON)      ? true
                                 : false;
}

FOLLY_ALWAYS_INLINE std::unordered_set<std::string> splitToStrSet(
    std::string commaSplitStr) {
  std::unordered_set<std::string> result;
  std::istringstream ss(commaSplitStr);
  std::string token;

  // Split by comma and add each token to the hashset
  while (std::getline(ss, token, ',')) {
    result.insert(token);
  }
  return result;
}

FOLLY_ALWAYS_INLINE bool checkKernelVersion(int major, int minor, int patch) {
  struct utsname buffer;

  if (uname(&buffer) != 0) {
    // Error handling
    return false;
  }

  std::string release(buffer.release);
  std::vector<int> versionParts;
  std::stringstream ss(release);
  std::string item;

  // Parse version string like "5.4.0-42-generic"
  while (std::getline(ss, item, '.')) {
    try {
      // Only capture digits until non-digit character
      size_t pos = 0;
      int value = std::stoi(item, &pos);
      versionParts.push_back(value);

      // If we have enough parts, break
      if (versionParts.size() >= 3) {
        break;
      }
    } catch (...) {
      break;
    }
  }

  // Make sure we have at least major version
  if (versionParts.empty()) {
    return false;
  }

  // Compare versions
  if (versionParts[0] > major) {
    return true;
  } else if (versionParts[0] < major) {
    return false;
  }

  // Major versions are equal, check minor
  if (versionParts.size() > 1) {
    if (versionParts[1] > minor) {
      return true;
    } else if (versionParts[1] < minor) {
      return false;
    }
  } else {
    return minor == 0 && patch == 0;
  }

  // Minor versions are equal, check patch
  if (versionParts.size() > 2) {
    return versionParts[2] >= patch;
  }

  return patch == 0;
}

} // namespace bytedance::bolt::config

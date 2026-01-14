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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <re2/re2.h>

#include "bolt/common/config/Config.h"
namespace bytedance::bolt::config {

double toBytesPerCapacityUnit(CapacityUnit unit) {
  switch (unit) {
    case CapacityUnit::BYTE:
      return 1;
    case CapacityUnit::KILOBYTE:
      return exp2(10);
    case CapacityUnit::MEGABYTE:
      return exp2(20);
    case CapacityUnit::GIGABYTE:
      return exp2(30);
    case CapacityUnit::TERABYTE:
      return exp2(40);
    case CapacityUnit::PETABYTE:
      return exp2(50);
    default:
      BOLT_USER_FAIL("Invalid capacity unit '{}'", (int)unit);
  }
}

CapacityUnit valueOfCapacityUnit(const std::string& unitStr) {
  std::stringstream ss;
  for (const char c : unitStr) {
    ss << static_cast<char>(std::tolower(c));
  }
  const auto lowerUnitStr = ss.str();
  if (lowerUnitStr == "b") {
    return CapacityUnit::BYTE;
  }
  if (lowerUnitStr == "kb") {
    return CapacityUnit::KILOBYTE;
  }
  if (lowerUnitStr == "mb") {
    return CapacityUnit::MEGABYTE;
  }
  if (lowerUnitStr == "gb") {
    return CapacityUnit::GIGABYTE;
  }
  if (lowerUnitStr == "tb") {
    return CapacityUnit::TERABYTE;
  }
  if (lowerUnitStr == "pb") {
    return CapacityUnit::PETABYTE;
  }
  BOLT_USER_FAIL("Invalid capacity unit '{}'", unitStr);
}

uint64_t toCapacity(const std::string& from, CapacityUnit to) {
  static const RE2 kPattern(R"(^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]*)\s*$)");
  double value;
  std::string unit;
  if (!RE2::FullMatch(from, kPattern, &value, &unit)) {
    BOLT_USER_FAIL("Invalid capacity string '{}'", from);
  }
  std::string actualUnit = unit.empty() ? "b" : unit;

  return value *
      (toBytesPerCapacityUnit(valueOfCapacityUnit(actualUnit)) /
       toBytesPerCapacityUnit(to));
}

std::chrono::duration<double> toDuration(const std::string& str) {
  static const RE2 kPattern(R"(^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*)");

  double value;
  std::string unit;
  if (!RE2::FullMatch(str, kPattern, &value, &unit)) {
    BOLT_USER_FAIL("Invalid duration '{}'", str);
  }
  if (unit == "ns") {
    return std::chrono::duration<double, std::nano>(value);
  } else if (unit == "us") {
    return std::chrono::duration<double, std::micro>(value);
  } else if (unit == "ms") {
    return std::chrono::duration<double, std::milli>(value);
  } else if (unit == "s") {
    return std::chrono::duration<double>(value);
  } else if (unit == "m") {
    return std::chrono::duration<double, std::ratio<60>>(value);
  } else if (unit == "h") {
    return std::chrono::duration<double, std::ratio<60 * 60>>(value);
  } else if (unit == "d") {
    return std::chrono::duration<double, std::ratio<60 * 60 * 24>>(value);
  }
  BOLT_USER_FAIL("Invalid duration '{}'", str);
}

ConfigBase& ConfigBase::set(const std::string& key, const std::string& val) {
  BOLT_CHECK(mutable_, "Cannot set in immutable config");
  std::unique_lock<std::shared_mutex> l(mutex_);
  configs_[key] = val;
  return *this;
}

ConfigBase& ConfigBase::reset() {
  BOLT_CHECK(mutable_, "Cannot reset in immutable config");
  std::unique_lock<std::shared_mutex> l(mutex_);
  configs_.clear();
  return *this;
}

bool ConfigBase::valueExists(const std::string& key) const {
  std::shared_lock<std::shared_mutex> l(mutex_);
  return configs_.find(key) != configs_.end();
};

const std::unordered_map<std::string, std::string>& ConfigBase::rawConfigs()
    const {
  BOLT_CHECK(
      !mutable_,
      "Mutable config cannot return unprotected reference to raw configs.");
  return configs_;
}

std::unordered_map<std::string, std::string> ConfigBase::rawConfigsCopy()
    const {
  std::shared_lock<std::shared_mutex> l(mutex_);
  return configs_;
}

folly::Optional<std::string> ConfigBase::get(const std::string& key) const {
  folly::Optional<std::string> val;
  std::shared_lock<std::shared_mutex> l(mutex_);
  auto it = configs_.find(key);
  if (it != configs_.end()) {
    val = it->second;
  }
  return val;
}
} // namespace bytedance::bolt::config

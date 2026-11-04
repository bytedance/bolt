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

#include "bolt/functions/lib/DateTimeFormatter.h"

namespace bytedance::bolt::functions {

// One cache belongs to one expression with fixed formatter options and context.
class DateTimeFormatterCache {
 public:
  static constexpr size_t kMaxEntries = 4;
  static constexpr size_t kMaxPatternSize = 1024;

  template <typename Factory>
  std::shared_ptr<DateTimeFormatter> get(
      std::string_view pattern,
      Factory&& factory,
      bool cacheTimeDependent = true) {
    if (pattern.size() > kMaxPatternSize) {
      return factory();
    }
    for (const auto& entry : entries_) {
      if (entry.pattern == pattern) {
        return entry.formatter;
      }
    }
    auto formatter = factory();
    if (!cacheTimeDependent && formatter &&
        formatter->dependsOnCreationTime()) {
      return formatter;
    }
    Entry entry{std::string(pattern), formatter};
    if (entries_.size() < kMaxEntries) {
      if (entries_.empty()) {
        entries_.reserve(kMaxEntries);
      }
      entries_.push_back(std::move(entry));
    } else {
      entries_[next_] = std::move(entry);
      next_ = (next_ + 1) % kMaxEntries;
    }
    return formatter;
  }

 private:
  struct Entry {
    std::string pattern;
    std::shared_ptr<DateTimeFormatter> formatter;
  };
  std::vector<Entry> entries_;
  size_t next_{0};
};

} // namespace bytedance::bolt::functions

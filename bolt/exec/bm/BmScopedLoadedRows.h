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

#include <cstddef>
#include <cstdint>
#include <vector>

namespace bytedance::bolt::exec::bm {

class ReadOnlyWindowReadSession;

// Keeps the chunks backing a batch of row pointers pinned until destruction or
// reset. The session and container must outlive the batch. While a batch is
// active, do not mutate the container, perform consuming reads, or acquire
// pins through another session.
class ScopedLoadedRows {
 public:
  ScopedLoadedRows() = default;
  ~ScopedLoadedRows();
  ScopedLoadedRows(ScopedLoadedRows&& other) noexcept;
  ScopedLoadedRows& operator=(ScopedLoadedRows&& other) noexcept;
  ScopedLoadedRows(const ScopedLoadedRows&) = delete;
  ScopedLoadedRows& operator=(const ScopedLoadedRows&) = delete;

  // For resident/bulk paths whose pins are managed by the existing owner.
  static ScopedLoadedRows borrowed(std::vector<const char*> rows);

  const char* const* data() const& {
    return rows_.data();
  }
  const char* const* data() const&& = delete;

  size_t size() const {
    return rows_.size();
  }
  bool empty() const {
    return rows_.empty();
  }

  void reset() noexcept;

 private:
  ReadOnlyWindowReadSession* session_{nullptr};
  std::vector<const char*> rows_;
  std::vector<uint64_t> chunkKeys_;

  friend class ReadOnlyWindowReadSession;
};

} // namespace bytedance::bolt::exec::bm

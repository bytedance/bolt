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

#include "bolt/common/memory/bm/file/ManagedOpenFile.h"

#include <filesystem>
#include <utility>

namespace bytedance::bolt::memory::bm {

ManagedOpenFile::ManagedOpenFile(std::string path, int fd)
    : path_(std::move(path)), fd_(fd) {}

ManagedOpenFile::~ManagedOpenFile() {
  Close();
}

ManagedOpenFile::ManagedOpenFile(ManagedOpenFile&& other) noexcept
    : path_(std::move(other.path_)), fd_(std::move(other.fd_)) {}

ManagedOpenFile& ManagedOpenFile::operator=(ManagedOpenFile&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  Close();
  path_ = std::move(other.path_);
  fd_ = std::move(other.fd_);
  return *this;
}

void ManagedOpenFile::Close() {
  fd_.reset();
}

void ManagedOpenFile::CloseAndRemove() {
  Close();
  if (!path_.empty()) {
    std::filesystem::remove(path_);
  }
}

} // namespace bytedance::bolt::memory::bm

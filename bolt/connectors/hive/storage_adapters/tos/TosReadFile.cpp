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

#include "bolt/connectors/hive/storage_adapters/tos/TosReadFile.h"

#include <cstring>
#include <limits>

namespace bytedance::bolt {

TosReadFile::TosReadFile(
    const std::shared_ptr<VolcengineTos::TosClientV2>& tosClient,
    const std::string_view& bucketName,
    const std::string_view& objectName)
    : tosClient_(tosClient), bucketName_(bucketName), objectName_(objectName) {
  VolcengineTos::HeadObjectV2Input headInput(bucketName_, objectName_);
  auto outcome = tosClient_->headObject(headInput);
  BOLT_CHECK(
      outcome.isSuccess(),
      "Unable to get file path info for file: {}{}. errorMsg = {}",
      bucketName_,
      objectName_,
      outcome.error().String());
  BOLT_CHECK_GE(
      outcome.result().getContentLength(), 0, "Invalid TOS object size");
  fileSize_ = outcome.result().getContentLength();
}

void TosReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  checkFileReadParameters(offset, length);
  if (length == 0) {
    return;
  }
  BOLT_CHECK_NOT_NULL(pos, "TOS read buffer must not be null");
  VolcengineTos::GetObjectV2Input input_obj_get(bucketName_, objectName_);
  input_obj_get.setRangeStart(offset);
  input_obj_get.setRangeEnd(offset + length - 1);

  auto outcome = tosClient_->getObject(input_obj_get);
  BOLT_CHECK(
      outcome.isSuccess(),
      "Fail to read tos object: {}{}:{}+{}. errorMsg = {}",
      bucketName_,
      objectName_,
      offset,
      length,
      outcome.error().String());
  BOLT_CHECK(
      outcome.result().getContentLength() == length,
      "Fail to read tos object: {}{}:{}+{}. total bytes read {}",
      bucketName_,
      objectName_,
      offset,
      length,
      outcome.result().getContentLength());
  auto stream = outcome.result().getContent();
  BOLT_CHECK_NOT_NULL(stream, "Missing TOS response stream");
  uint64_t totalBytesRead = 0;
  while (totalBytesRead < length) {
    BOLT_CHECK(
        stream->good(),
        "Fail to read tos object: {}{}:{}+{}. total bytes read {}",
        bucketName_,
        objectName_,
        offset,
        length,
        totalBytesRead);
    stream->read(pos, length - totalBytesRead);
    auto bytesRead = stream->gcount();
    BOLT_CHECK_GT(bytesRead, 0, "TOS response stream made no progress");
    totalBytesRead += bytesRead;
    pos += bytesRead;
  }
}

std::string_view TosReadFile::pread(uint64_t offset, uint64_t length, void* buf)
    const {
  preadInternal(offset, length, static_cast<char*>(buf));
  return {static_cast<char*>(buf), length};
}

std::string TosReadFile::pread(uint64_t offset, uint64_t length) const {
  checkFileReadParameters(offset, length);
  std::string result(length, 0);
  char* pos = result.data();
  preadInternal(offset, length, pos);
  return result;
}

uint64_t TosReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  // 'buffers' contains Ranges(data, size) with some gaps (data = nullptr) in
  // between. This call must populate the ranges (except gap ranges)
  // sequentially starting from 'offset'. Tos does not support multi-range and
  // charges by number of read requests rather than size. Use a single read
  // spanning all ranges and then populate individual ranges.
  checkFileReadParameters(offset, 0);
  uint64_t length = 0;
  for (const auto range : buffers) {
    BOLT_CHECK_LE(
        range.size(),
        fileSize_ - offset - length,
        "TOS vector read exceeds object size");
    length += range.size();
  }
  if (length == 0) {
    return 0;
  }
  std::string result(length, 0);
  preadInternal(offset, length, result.data());
  size_t resultOffset = 0;
  for (auto range : buffers) {
    if (range.data()) {
      memcpy(range.data(), result.data() + resultOffset, range.size());
    }
    resultOffset += range.size();
  }
  return length;
}

uint64_t TosReadFile::size() const {
  return fileSize_;
}

uint64_t TosReadFile::memoryUsage() const {
  return sizeof(TosReadFile);
}

bool TosReadFile::shouldCoalesce() const {
  return false;
}

void TosReadFile::checkFileReadParameters(uint64_t offset, uint64_t length)
    const {
  BOLT_CHECK_LE(offset, fileSize_, "TOS read offset exceeds object size");
  BOLT_CHECK_LE(length, fileSize_ - offset, "TOS read exceeds object size");
}
} // namespace bytedance::bolt

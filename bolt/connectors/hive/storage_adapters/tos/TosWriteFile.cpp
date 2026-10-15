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

#include "bolt/connectors/hive/storage_adapters/tos/TosWriteFile.h"

#include <limits>
#include <streambuf>

#include <model/object/AppendObjectV2Input.h>
#include <model/object/DeleteObjectInput.h>
#include <model/object/HeadObjectV2Input.h>
#include <model/object/PutObjectV2Input.h>

#include "bolt/common/base/SuccinctPrinter.h"

namespace bytedance::bolt {

namespace {
// The SDK seeks back to the initial position when retrying a request. Keep the
// caller's buffer alive for the synchronous SDK call without copying it.
class StringViewStreamBuf : public std::streambuf {
 public:
  explicit StringViewStreamBuf(std::string_view data) {
    auto* begin = const_cast<char*>(data.empty() ? "" : data.data());
    setg(begin, begin, begin + data.size());
  }

 protected:
  pos_type seekoff(
      off_type offset,
      std::ios_base::seekdir direction,
      std::ios_base::openmode mode) override {
    if (!(mode & std::ios_base::in) || (mode & std::ios_base::out)) {
      return pos_type(off_type(-1));
    }
    off_type base = 0;
    if (direction == std::ios_base::cur) {
      base = gptr() - eback();
    } else if (direction == std::ios_base::end) {
      base = egptr() - eback();
    } else if (direction != std::ios_base::beg) {
      return pos_type(off_type(-1));
    }
    if (offset < -base || offset > (egptr() - eback()) - base) {
      return pos_type(off_type(-1));
    }
    setg(eback(), eback() + base + offset, egptr());
    return pos_type(base + offset);
  }

  pos_type seekpos(pos_type position, std::ios_base::openmode mode) override {
    return seekoff(off_type(position), std::ios_base::beg, mode);
  }
};

class StringViewStream : private StringViewStreamBuf, public std::iostream {
 public:
  explicit StringViewStream(std::string_view data)
      : StringViewStreamBuf(data), std::iostream(this) {}
};
} // namespace

TosWriteFile::TosWriteFile(
    const std::shared_ptr<VolcengineTos::TosClientV2>& tosClient,
    const std::string_view& bucketName,
    const std::string_view& objectName,
    BucketType bucketType,
    bool shouldThrowOnFileAlreadyExists)
    : tosClient_(tosClient),
      bucketName_(bucketName),
      objectName_(objectName),
      shouldThrowOnFileAlreadyExists_(shouldThrowOnFileAlreadyExists),
      bucketType_(bucketType),
      appendObjectV2Input_(std::make_unique<VolcengineTos::AppendObjectV2Input>(
          bucketName_,
          objectName_,
          nullptr,
          0)) {
  if (shouldThrowOnFileAlreadyExists_) {
    BOLT_CHECK(
        !isObjectExist(), "Object already exists, path: {}", objectName_);
    return;
  }

  // Overwrite semantics. On HNS the first write is a PutObject which already
  // overwrites any existing object, so nothing to do here. On FNS the write
  // path is AppendObject from offset 0, which only succeeds against a
  // non-existent object; remove any pre-existing object first so overwrite has
  // consistent behavior across bucket types.
  if (bucketType_ == BucketType::FNS && isObjectExist()) {
    deleteObject();
  }
}

void TosWriteFile::append(std::string_view data) {
  BOLT_CHECK(!isClosed_, "Cannot append to a closed Tos file: {}", objectName_);
  BOLT_CHECK_LE(
      data.size(),
      std::numeric_limits<int64_t>::max() - nextAppendOffset_,
      "TOS file size exceeds supported range");
  if (data.empty()) {
    return;
  }

  if (bucketType_ == BucketType::HNS && !hasWritten_) {
    putObject(data);
  } else {
    appendObject(data);
  }
  hasWritten_ = true;
}

void TosWriteFile::putObject(std::string_view data) {
  VolcengineTos::PutObjectV2Input input(
      bucketName_, objectName_, std::make_shared<StringViewStream>(data));
  input.setContentLength(data.size());
  auto result = tosClient_->putObject(input);
  BOLT_CHECK(
      result.isSuccess(),
      "Tos putObject failed, msg: {}, objectName: {}, bucketName: {}.",
      result.error().String(),
      objectName_,
      bucketName_);
  nextAppendOffset_ = data.size();
  preHashCrc64ecma_ = result.result().getHashCrc64ecma();
}

void TosWriteFile::appendObject(std::string_view data) {
  appendObjectV2Input_->setContent(std::make_shared<StringViewStream>(data));
  appendObjectV2Input_->setContentLength(data.size());
  appendObjectV2Input_->setOffset(nextAppendOffset_);
  appendObjectV2Input_->setPreHashCrc64Ecma(preHashCrc64ecma_);
  auto result = tosClient_->appendObject(*appendObjectV2Input_);
  BOLT_CHECK(
      result.isSuccess(),
      "Tos appendObject failed, msg: {}, objectName: {}, bucketName: {}, offset: {}.",
      result.error().String(),
      objectName_,
      bucketName_,
      nextAppendOffset_);
  BOLT_CHECK_EQ(
      result.result().getNextAppendOffset(),
      nextAppendOffset_ + data.size(),
      "Unexpected TOS append offset");
  nextAppendOffset_ = result.result().getNextAppendOffset();
  preHashCrc64ecma_ = result.result().getHashCrc64ecma();
}

void TosWriteFile::close() {
  if (isClosed_) {
    return;
  }
  if (!hasWritten_) {
    putObject("");
  }
  isClosed_ = true;
  LOG(INFO) << "closed Tos object, name: " << objectName_
            << ", bucket: " << bucketName_
            << ", size: " << succinctBytes(nextAppendOffset_);
}

uint64_t TosWriteFile::size() const {
  return nextAppendOffset_;
}

bool TosWriteFile::isObjectExist() {
  VolcengineTos::HeadObjectV2Input input(bucketName_, objectName_);
  auto result = tosClient_->headObject(input);
  if (result.isSuccess()) {
    return true;
  }
  // Only a definitive server-side 404 means the object is absent. Any other
  // failure (client/network error, timeout, 403 permission denied, 5xx, ...)
  // is ambiguous: treating it as "does not exist" could silently overwrite an
  // object we simply failed to inspect, so surface it instead.
  auto error = result.error();
  if (!error.isClientError() && error.getStatusCode() == 404) {
    return false;
  }
  BOLT_FAIL(
      "Tos headObject failed while checking object existence, msg: {}, objectName: {}, bucketName: {}.",
      error.String(),
      objectName_,
      bucketName_);
}

void TosWriteFile::deleteObject() {
  VolcengineTos::DeleteObjectInput input(bucketName_, objectName_);
  auto result = tosClient_->deleteObject(input);
  BOLT_CHECK(
      result.isSuccess(),
      "Tos deleteObject failed, msg: {}, objectName: {}, bucketName: {}.",
      result.error().String(),
      objectName_,
      bucketName_);
}

} // namespace bytedance::bolt

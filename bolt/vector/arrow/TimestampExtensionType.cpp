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
#include "bolt/vector/arrow/TimestampExtensionType.h"

#include <arrow/extension_type.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt {
namespace {

class TimestampExtensionType final : public arrow::ExtensionType {
 public:
  explicit TimestampExtensionType(
      std::shared_ptr<arrow::DataType> storageType =
          arrow::fixed_size_binary(16))
      : ExtensionType(std::move(storageType)) {}

  std::string extension_name() const override {
    return kTimestampExtensionName;
  }

  bool ExtensionEquals(const arrow::ExtensionType& other) const override {
    return other.extension_name() == extension_name() &&
        other.storage_type()->Equals(storage_type()) &&
        other.Serialize() == Serialize();
  }

  std::shared_ptr<arrow::Array> MakeArray(
      std::shared_ptr<arrow::ArrayData> data) const override {
    return std::make_shared<arrow::ExtensionArray>(data);
  }

  arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
      std::shared_ptr<arrow::DataType> storageType,
      const std::string& metadata) const override {
    // IPC attaches extension metadata to the dictionary field. C Data can
    // attach it to the dictionary values instead; accept both representations.
    const auto* valueType = storageType.get();
    if (valueType->id() == arrow::Type::DICTIONARY) {
      valueType = static_cast<const arrow::DictionaryType*>(valueType)
                      ->value_type()
                      .get();
    }
    if (valueType->id() != arrow::Type::FIXED_SIZE_BINARY ||
        static_cast<const arrow::FixedSizeBinaryType*>(valueType)
                ->byte_width() != 16) {
      return arrow::Status::Invalid(
          "bolt.timestamp requires FixedSizeBinary(16)");
    }
    if (metadata != kTimestampExtensionMetadata) {
      return arrow::Status::Invalid("Unsupported bolt.timestamp version");
    }
    return std::make_shared<TimestampExtensionType>(std::move(storageType));
  }

  std::string Serialize() const override {
    return kTimestampExtensionMetadata;
  }
};

} // namespace

void registerArrowTimestampType() {
  static const bool registered = [] {
    auto type = std::make_shared<TimestampExtensionType>();
    const auto status = arrow::RegisterExtensionType(type);
    if (!status.ok()) {
      const auto existing = arrow::GetExtensionType(kTimestampExtensionName);
      BOLT_CHECK(
          existing && type->ExtensionEquals(*existing),
          "Cannot register bolt.timestamp: {}",
          status.ToString());
    }
    return true;
  }();
  (void)registered;
}

} // namespace bytedance::bolt

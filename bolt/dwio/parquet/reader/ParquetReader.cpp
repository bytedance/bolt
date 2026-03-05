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

#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include <parquet/metadata.h>
#include <thrift/protocol/TCompactProtocol.h> //@manual
#include <cstdint>
#include "bolt/dwio/common/CachedBufferedInput.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/parquet/encryption/KmsClient.h"
#include "bolt/dwio/parquet/reader/ParquetColumnReader.h"
#include "bolt/dwio/parquet/reader/ParquetFooterCache.h"
#include "bolt/dwio/parquet/reader/SchemaHelper.h"
#include "bolt/dwio/parquet/reader/StructColumnReader.h"
#include "bolt/dwio/parquet/thrift/FmtParquetFormatters.h"
#include "bolt/dwio/parquet/thrift/ThriftInternal.h"
#include "bolt/dwio/parquet/thrift/ThriftTransport.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"

namespace bytedance::bolt::parquet {

constexpr std::string_view kDcmapColPrefix =
    "parquet.meta.dynamic.column.map.keys.of.";

namespace {
::parquet::EncryptionAlgorithm FromThrift(
    const thrift::EncryptionAlgorithm& encryption) {
  ::parquet::EncryptionAlgorithm algo;
  if (encryption.__isset.AES_GCM_V1) {
    algo.algorithm = ::parquet::ParquetCipher::AES_GCM_V1;
    algo.aad = ::parquet::AadMetadata{
        encryption.AES_GCM_V1.aad_prefix,
        encryption.AES_GCM_V1.aad_file_unique,
        encryption.AES_GCM_V1.supply_aad_prefix};
  } else {
    algo.algorithm = ::parquet::ParquetCipher::AES_GCM_CTR_V1;
    algo.aad = ::parquet::AadMetadata{
        encryption.AES_GCM_CTR_V1.aad_prefix,
        encryption.AES_GCM_CTR_V1.aad_file_unique,
        encryption.AES_GCM_CTR_V1.supply_aad_prefix};
  }
  return algo;
}
} // namespace

/// Metadata and options for reading Parquet.
class ReaderBase {
 public:
  ReaderBase(
      std::unique_ptr<dwio::common::BufferedInput>,
      const dwio::common::ReaderOptions& options);

  virtual ~ReaderBase() = default;

  memory::MemoryPool& getMemoryPool() const {
    return pool_;
  }

  dwio::common::BufferedInput& bufferedInput() const {
    return *input_;
  }

  uint64_t fileLength() const {
    return fileLength_;
  }

  const thrift::FileMetaData& thriftFileMetaData() const {
    return *fileMetaData_;
  }

  const std::shared_ptr<InternalFileDecryptor>& fileDecryptor() const {
    return fileDecryptor_;
  }

  FileMetaDataPtr fileMetaData() const {
    return FileMetaDataPtr(reinterpret_cast<const void*>(fileMetaData_.get()));
  }

  const std::shared_ptr<const RowType>& schema() const {
    return schema_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& schemaWithId() {
    return schemaWithId_;
  }

  const std::unordered_map<std::string, thrift::LogicalType>&
  schemaLogicalTypes() const {
    return logicalTypesByPath_;
  }

  const std::unordered_map<std::string, thrift::ConvertedType::type>&
  schemaConvertedTypes() const {
    return convertedTypesByPath_;
  }

  bool isFileColumnNamesReadAsLowerCase() const {
    return options_.isFileColumnNamesReadAsLowerCase();
  }

  const dwio::common::ReaderOptions& options() const {
    return options_;
  }

  /// Ensures that streams are enqueued and loading for the row group at
  /// 'currentGroup'. May start loading one or more subsequent groups.
  void scheduleRowGroups(
      const std::vector<uint32_t>& groups,
      int32_t currentGroup,
      StructColumnReader& reader);

  /// Returns the uncompressed size for columns in 'type' and its children in
  /// row group.
  int64_t estimatedRowGroupBytesInMemory(
      int32_t rowGroupIndex,
      const dwio::common::TypeWithId& type) const;

  /// Checks whether the specific row group has been loaded and
  /// the data still exists in the buffered inputs.
  bool isRowGroupBuffered(int32_t rowGroupIndex) const;

  static std::shared_ptr<const dwio::common::TypeWithId> createTypeWithId(
      const std::shared_ptr<const dwio::common::TypeWithId>& inputType,
      const RowTypePtr& rowTypePtr,
      bool fileColumnNamesReadAsLowerCase);

 private:
  // Reads and parses file footer.
  void loadFileMetaData();

  void initializeSchema();

  std::shared_ptr<const ParquetTypeWithId> getParquetColumnInfo(
      uint32_t maxSchemaElementIdx,
      uint32_t maxRepeat,
      uint32_t maxDefine,
      uint32_t parentSchemaIdx,
      uint32_t& schemaIdx,
      uint32_t& columnIdx,
      const TypePtr& requestedType,
      const TypePtr& parentRequestedType) const;

  TypePtr convertType(
      const thrift::SchemaElement& schemaElement,
      const TypePtr& requestedType) const;

  static std::shared_ptr<const RowType> createRowType(
      std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>>
          children,
      bool fileColumnNamesReadAsLowerCase);

  uint32_t parseEncryptedFooter(
      const char* cryptoMetadataBuffer,
      uint32_t footerLen);
  void parsePlainFooter();
  std::string HandleAadPrefix(
      const std::shared_ptr<::parquet::FileDecryptionProperties>&
          fileDecryptionProperties,
      ::parquet::EncryptionAlgorithm& algo);

  memory::MemoryPool& pool_;
  const uint64_t footerEstimatedSize_;
  const uint64_t filePreloadThreshold_;
  // Copy of options. Must be owned by 'this'.
  const dwio::common::ReaderOptions options_;
  std::shared_ptr<bolt::dwio::common::BufferedInput> input_;
  uint64_t fileLength_;
  std::shared_ptr<thrift::FileMetaData> fileMetaData_;
  RowTypePtr schema_;
  std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;

  // Logical types keyed by dot-delimited field path from the root.
  std::unordered_map<std::string, thrift::LogicalType> logicalTypesByPath_;

  std::unordered_map<std::string, thrift::ConvertedType::type>
      convertedTypesByPath_;

  // Map from row group index to pre-created loading BufferedInput.
  std::unordered_map<uint32_t, std::shared_ptr<dwio::common::BufferedInput>>
      inputs_;

  std::shared_ptr<::parquet::encryption::CryptoFactory> cryptoFactory_;

  std::shared_ptr<::parquet::FileDecryptionProperties>
      fileDecryptionProperties_;
  std::shared_ptr<InternalFileDecryptor> fileDecryptor_;

  void collectLogicalTypes(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const std::string& path);

  void collectConvertedTypes(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const std::string& path);
};

ReaderBase::ReaderBase(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : pool_(options.getMemoryPool()),
      footerEstimatedSize_(options.getFooterEstimatedSize()),
      filePreloadThreshold_(options.getFilePreloadThreshold()),
      options_(options),
      input_(std::move(input)) {
  fileLength_ = input_->getReadFile()->size();
  BOLT_CHECK_GT(fileLength_, 0, "Parquet file is empty");
  BOLT_CHECK_GE(fileLength_, 12, "Parquet file is too small");

  // TODO: move from class construction
  uint64_t loadFileMetaDataTimeNs = 0;
  {
    NanosecondTimer timer(&loadFileMetaDataTimeNs);
    loadFileMetaData();
  }
  auto ioStats = input_->getInputStream()->getStats();
  if (ioStats) {
    ioStats->incLoadFileMetaDataTimeNs(loadFileMetaDataTimeNs);
  }
  initializeSchema();
}

void ReaderBase::loadFileMetaData() {
  bool preloadFile = (dynamic_cast<dwio::common::CachedBufferedInput*>(
                          input_.get()) == nullptr) &&
      (fileLength_ <= std::max(filePreloadThreshold_, footerEstimatedSize_));
  uint64_t readSize =
      preloadFile ? fileLength_ : std::min(fileLength_, footerEstimatedSize_);

  if (auto* cache = ParquetFooterCache::getInstance()) {
    auto entry = cache->get(input_->getReadFile()->getName());
    if (entry.has_value()) {
      fileMetaData_ = std::move(entry.value());
      parsePlainFooter();
      return;
    }
  }

  std::unique_ptr<dwio::common::SeekableInputStream> stream;
  if (preloadFile) {
    stream = input_->loadCompleteFile();
  } else {
    stream = input_->read(
        fileLength_ - readSize, readSize, dwio::common::LogType::FOOTER);
  }

  std::vector<char> copy(readSize);
  const char* bufferStart = nullptr;
  const char* bufferEnd = nullptr;
  dwio::common::readBytes(
      readSize, stream.get(), copy.data(), bufferStart, bufferEnd);
  BOLT_CHECK(
      strncmp(copy.data() + readSize - 4, "PAR1", 4) == 0 ||
          strncmp(copy.data() + readSize - 4, "PARE", 4) == 0,
      "No magic bytes found at end of the Parquet file");
  const bool isEncryptedFooter =
      strncmp(copy.data() + readSize - 4, "PARE", 4) == 0;

  uint32_t footerLength =
      *(reinterpret_cast<const uint32_t*>(copy.data() + readSize - 8));
  BOLT_CHECK_LE(footerLength + 12, fileLength_);
  int32_t footerOffsetInBuffer = readSize - 8 - footerLength;
  if (footerLength > readSize - 8) {
    footerOffsetInBuffer = 0;
    auto missingLength = footerLength - readSize + 8;
    stream = input_->read(
        fileLength_ - footerLength - 8,
        missingLength,
        dwio::common::LogType::FOOTER);
    copy.resize(footerLength);
    std::memmove(copy.data() + missingLength, copy.data(), readSize - 8);
    bufferStart = nullptr;
    bufferEnd = nullptr;
    dwio::common::readBytes(
        missingLength, stream.get(), copy.data(), bufferStart, bufferEnd);
  }

  char* fileMetadataBuffer = copy.data() + footerOffsetInBuffer;
  uint32_t fileMetadataLen = footerLength;
  std::vector<char> decryptedBuffer;

  if (isEncryptedFooter) {
    // Encrypted file with Encrypted footer.
    uint32_t cryptoMetadataLen =
        parseEncryptedFooter(copy.data() + footerOffsetInBuffer, footerLength);
    // Read the actual footer
    footerOffsetInBuffer += cryptoMetadataLen;
    footerLength -= cryptoMetadataLen;
    auto decryptor = fileDecryptor_->GetFooterDecryptor();
    decryptedBuffer.resize(footerLength - decryptor->CiphertextSizeDelta());
    int32_t decryptedBufferLen = decryptor->Decrypt(
        reinterpret_cast<uint8_t*>(copy.data() + footerOffsetInBuffer),
        0,
        reinterpret_cast<uint8_t*>(decryptedBuffer.data()),
        decryptedBuffer.size());
    if (decryptedBufferLen <= 0) {
      BOLT_FAIL("Couldn't decrypt footer buffer");
    }
    fileMetadataBuffer = decryptedBuffer.data();
    fileMetadataLen = decryptedBufferLen;
  }

  std::shared_ptr<thrift::ThriftTransport> thriftTransport =
      std::make_shared<thrift::ThriftBufferedTransport>(
          fileMetadataBuffer, fileMetadataLen);
  auto thriftProtocol = std::make_unique<
      apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport>>(
      thriftTransport);
  fileMetaData_ = std::make_shared<thrift::FileMetaData>();
  fileMetaData_->read(thriftProtocol.get());

  if (isEncryptedFooter) {
    return;
  } else if (!fileMetaData_->__isset.encryption_algorithm) {
    if (fileDecryptionProperties_ != nullptr) {
      if (!fileDecryptionProperties_->plaintext_files_allowed()) {
        BOLT_FAIL("Applying decryption properties on plaintext file");
      }
    }
  } else {
    // Encrypted file with plaintext footer mode.
    parsePlainFooter();
  }

  if (auto* cache = ParquetFooterCache::getInstance()) {
    cache->add(input_->getReadFile()->getName(), fileMetaData_, footerLength);
  }
}

void ReaderBase::initializeSchema() {
  BOLT_CHECK_GT(
      fileMetaData_->schema.size(),
      1,
      "Invalid Parquet schema: Need at least one non-root column in the file");

  // for parquet-go created parquet file
  fileMetaData_->schema[0].repetition_type =
      thrift::FieldRepetitionType::REQUIRED;

  BOLT_CHECK_GT(
      fileMetaData_->schema[0].num_children,
      0,
      "Invalid Parquet schema: root element must have at least 1 child");

  uint32_t maxDefine = 0;
  uint32_t maxRepeat = 0;
  uint32_t schemaIdx = 0;
  uint32_t columnIdx = 0;
  uint32_t maxSchemaElementIdx = fileMetaData_->schema.size() - 1;
  // Setting the parent schema index of the root("hive_schema") to be 0, which
  // is the root itself. This is ok because it's never required to check the
  // parent of the root in getParquetColumnInfo().
  schemaWithId_ = getParquetColumnInfo(
      maxSchemaElementIdx,
      maxRepeat,
      maxDefine,
      0,
      schemaIdx,
      columnIdx,
      options_.getFileSchema(),
      nullptr);
  schema_ = createRowType(
      schemaWithId_->getChildren(), isFileColumnNamesReadAsLowerCase());

  logicalTypesByPath_.clear();
  convertedTypesByPath_.clear();
  collectLogicalTypes(schemaWithId_, "");
  collectConvertedTypes(schemaWithId_, "");
}

std::shared_ptr<const ParquetTypeWithId> ReaderBase::getParquetColumnInfo(
    uint32_t maxSchemaElementIdx,
    uint32_t maxRepeat,
    uint32_t maxDefine,
    uint32_t parentSchemaIdx,
    uint32_t& schemaIdx,
    uint32_t& columnIdx,
    const TypePtr& requestedType,
    const TypePtr& parentRequestedType) const {
  BOLT_CHECK(fileMetaData_ != nullptr);
  BOLT_CHECK_LT(schemaIdx, fileMetaData_->schema.size());

  auto& schema = fileMetaData_->schema;
  uint32_t curSchemaIdx = schemaIdx;
  auto& schemaElement = schema[curSchemaIdx];
  bool isRepeated = false;
  bool isOptional = false;

  if (schemaElement.__isset.repetition_type) {
    if (schemaElement.repetition_type !=
        thrift::FieldRepetitionType::REQUIRED) {
      maxDefine++;
    }
    if (schemaElement.repetition_type ==
        thrift::FieldRepetitionType::REPEATED) {
      maxRepeat++;
      isRepeated = true;
    }
    if (schemaElement.repetition_type ==
        thrift::FieldRepetitionType::OPTIONAL) {
      isOptional = true;
    }
  }

  auto name = schemaElement.name;
  if (isFileColumnNamesReadAsLowerCase()) {
    folly::toLowerAscii(name);
  }
  if (!schemaElement.__isset.type) { // inner node
    BOLT_CHECK(
        schemaElement.__isset.num_children && schemaElement.num_children > 0,
        "Node has no children but should");
    BOLT_CHECK(
        !requestedType || requestedType->isRow() || requestedType->isArray() ||
        requestedType->isMap());

    std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>> children;

    auto curSchemaIdx = schemaIdx;
    for (int32_t i = 0; i < schemaElement.num_children; i++) {
      ++schemaIdx;
      auto childName = schema[schemaIdx].name;
      if (isFileColumnNamesReadAsLowerCase()) {
        folly::toLowerAscii(childName);
      }

      TypePtr childRequestedType = nullptr;
      if (requestedType && requestedType->isRow()) {
        auto fileTypeIdx =
            requestedType->asRow().getChildIdxIfExists(childName);
        if (fileTypeIdx.has_value()) {
          childRequestedType = requestedType->asRow().childAt(*fileTypeIdx);
        }
      }

      // Handling elements of ARRAY/MAP
      if (!requestedType && parentRequestedType) {
        if (parentRequestedType->isArray()) {
          childRequestedType = parentRequestedType->asArray().elementType();
        } else if (parentRequestedType->isMap()) {
          auto mapType = parentRequestedType->asMap();
          // Processing map keys
          if (i == 0) {
            childRequestedType = mapType.keyType();
          } else {
            childRequestedType = mapType.valueType();
          }
        }
      }

      auto child = getParquetColumnInfo(
          maxSchemaElementIdx,
          maxRepeat,
          maxDefine,
          curSchemaIdx,
          schemaIdx,
          columnIdx,
          childRequestedType,
          requestedType);
      children.push_back(std::move(child));
    }
    BOLT_CHECK(!children.empty());

    if (schemaElement.__isset.converted_type) {
      switch (schemaElement.converted_type) {
        case thrift::ConvertedType::LIST: {
          BOLT_CHECK_EQ(children.size(), 1);
          const auto& child = children[0];
          isRepeated = true;
          // In case the child is a MAP or current element is repeated then
          // wrap child around additional ARRAY
          if (child->type()->kind() == TypeKind::MAP ||
              schemaElement.repetition_type ==
                  thrift::FieldRepetitionType::REPEATED) {
            return std::make_unique<ParquetTypeWithId>(
                TypeFactory<TypeKind::ARRAY>::create(child->type()),
                std::move(children),
                curSchemaIdx,
                maxSchemaElementIdx,
                ParquetTypeWithId::kNonLeaf,
                std::move(name),
                std::nullopt,
                std::nullopt,
                std::nullopt,
                maxRepeat + 1,
                maxDefine,
                isOptional,
                isRepeated);
          }
          // Only special case list of map and list of list is handled here,
          // other generic case is handled with case MAP
          [[fallthrough]];
        }
        case thrift::ConvertedType::MAP_KEY_VALUE:
          // If the MAP_KEY_VALUE annotated group's parent is a MAP, it should
          // be the repeated key_value group that directly contains the key and
          // value children.
          if (schema[parentSchemaIdx].converted_type ==
              thrift::ConvertedType::MAP) {
            // TODO: the group names need to be checked. According to the spec,
            // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
            // the name of the schema element being 'key_value' is
            // also an indication of this is a map type
            BOLT_CHECK_EQ(
                schemaElement.repetition_type,
                thrift::FieldRepetitionType::REPEATED);
            BOLT_CHECK_EQ(children.size(), 2);

            auto childrenCopy = children;
            return std::make_shared<const ParquetTypeWithId>(
                TypeFactory<TypeKind::MAP>::create(
                    children[0]->type(), children[1]->type()),
                std::move(childrenCopy),
                curSchemaIdx, // TODO: there are holes in the ids
                maxSchemaElementIdx,
                ParquetTypeWithId::kNonLeaf, // columnIdx,
                std::move(name),
                std::nullopt,
                std::nullopt,
                std::nullopt,
                maxRepeat,
                maxDefine,
                isOptional,
                isRepeated);
          }

          // For backward-compatibility, a group annotated with MAP_KEY_VALUE
          // that is not contained by a MAP-annotated group should be handled as
          // a MAP-annotated group.
          [[fallthrough]];

        case thrift::ConvertedType::MAP: {
          BOLT_CHECK_EQ(children.size(), 1);
          const auto& child = children[0];
          auto grandChildren = child->getChildren();
          auto type = child->type();
          isRepeated = true;
          // This level will not have the "isRepeated" info in the parquet
          // schema since parquet schema will have a child layer which will have
          // the "repeated info" which we are ignoring here, hence we set the
          // isRepeated to true eg
          // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
          return std::make_unique<ParquetTypeWithId>(
              std::move(type),
              std::move(grandChildren),
              curSchemaIdx, // TODO: there are holes in the ids
              maxSchemaElementIdx,
              ParquetTypeWithId::kNonLeaf, // columnIdx,
              std::move(name),
              std::nullopt,
              std::nullopt,
              std::nullopt,
              maxRepeat + 1,
              maxDefine,
              isOptional,
              isRepeated);
        }

        case thrift::ConvertedType::DCMAP: {
          std::unordered_map<std::string, std::vector<std::string>> dcKeys;
          for (const auto& kv : fileMetaData_->key_value_metadata) {
            if (kv.key.find(kDcmapColPrefix, 0) != std::string::npos) {
              auto k = kv.key.substr(kv.key.rfind('.') + 1);
              auto v = kv.value;
              auto start = 0U;
              auto end = v.find(',');
              while (end != std::string::npos) {
                dcKeys[k].emplace_back(v.substr(start, end - start));
                start = end + 1;
                end = v.find(',', start);
              }
              dcKeys[k].push_back(v.substr(start));
            }
          }

          auto childrenCopy = children;
          return std::make_shared<const ParquetTypeWithId>(
              createRowType(children, isFileColumnNamesReadAsLowerCase()),
              std::move(childrenCopy),
              curSchemaIdx,
              maxSchemaElementIdx,
              ParquetTypeWithId::kNonLeaf, // columnIdx,
              std::move(name),
              std::nullopt,
              std::nullopt,
              std::nullopt,
              maxRepeat,
              maxDefine,
              isOptional,
              isRepeated,
              0,
              0,
              0,
              true,
              dcKeys);
        }

        default:
          BOLT_UNREACHABLE(
              "Invalid SchemaElement converted_type: {}, name: {}",
              schemaElement.converted_type,
              schemaElement.name);
      }
    } else {
      if (schemaElement.repetition_type ==
          thrift::FieldRepetitionType::REPEATED) {
        if (schema[parentSchemaIdx].converted_type ==
            thrift::ConvertedType::LIST) {
          // TODO: the group names need to be checked. According to spec,
          // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
          // the name of the schema element being 'array' is
          // also an indication of this is a list type
          // child of LIST
          BOLT_CHECK_GE(children.size(), 1);
          auto type = TypeFactory<TypeKind::ARRAY>::create(children[0]->type());
          return std::make_unique<ParquetTypeWithId>(
              std::move(type),
              std::move(children),
              curSchemaIdx,
              maxSchemaElementIdx,
              ParquetTypeWithId::kNonLeaf, // columnIdx,
              std::move(name),
              std::nullopt,
              std::nullopt,
              std::nullopt,
              maxRepeat,
              maxDefine,
              isOptional,
              isRepeated);
        } else if (
            schema[parentSchemaIdx].converted_type ==
                thrift::ConvertedType::MAP ||
            schema[parentSchemaIdx].converted_type ==
                thrift::ConvertedType::MAP_KEY_VALUE) {
          // children  of MAP
          BOLT_CHECK_EQ(children.size(), 2);
          auto type = TypeFactory<TypeKind::MAP>::create(
              children[0]->type(), children[1]->type());
          return std::make_unique<ParquetTypeWithId>(
              std::move(type),
              std::move(children),
              curSchemaIdx, // TODO: there are holes in the ids
              maxSchemaElementIdx,
              ParquetTypeWithId::kNonLeaf, // columnIdx,
              std::move(name),
              std::nullopt,
              std::nullopt,
              std::nullopt,
              maxRepeat,
              maxDefine,
              isOptional,
              isRepeated);
        } else if (
            schema[parentSchemaIdx].converted_type ==
            thrift::ConvertedType::DCMAP) {
          BOLT_CHECK_EQ(children.size(), 2);
          auto type = TypeFactory<TypeKind::MAP>::create(
              children[0]->type(), children[1]->type());
          return std::make_unique<ParquetTypeWithId>(
              std::move(type),
              std::move(children),
              curSchemaIdx,
              maxSchemaElementIdx,
              ParquetTypeWithId::kNonLeaf, // columnIdx,
              std::move(name),
              std::nullopt,
              std::nullopt,
              std::nullopt,
              maxRepeat,
              maxDefine - 1,
              isOptional,
              isRepeated);
        }
      } else {
        // Row type
        auto childrenCopy = children;
        return std::make_shared<const ParquetTypeWithId>(
            createRowType(children, isFileColumnNamesReadAsLowerCase()),
            std::move(childrenCopy),
            curSchemaIdx,
            maxSchemaElementIdx,
            ParquetTypeWithId::kNonLeaf, // columnIdx,
            std::move(name),
            std::nullopt,
            std::nullopt,
            std::nullopt,
            maxRepeat,
            maxDefine,
            isOptional,
            isRepeated);
      }
    }
  } else { // leaf node
    const auto boltType = convertType(schemaElement, requestedType);
    int32_t precision =
        schemaElement.__isset.precision ? schemaElement.precision : 0;
    int32_t scale = schemaElement.__isset.scale ? schemaElement.scale : 0;
    int32_t type_length =
        schemaElement.__isset.type_length ? schemaElement.type_length : 0;
    std::vector<std::shared_ptr<const dwio::common::TypeWithId>> children;
    const std::optional<thrift::LogicalType> logicalType_ =
        schemaElement.__isset.logicalType
        ? std::optional<thrift::LogicalType>(schemaElement.logicalType)
        : std::nullopt;
    const std::optional<thrift::ConvertedType::type> convertedType =
        schemaElement.__isset.converted_type
        ? std::optional<thrift::ConvertedType::type>(
              schemaElement.converted_type)
        : std::nullopt;

    std::shared_ptr<const ParquetTypeWithId> leafTypePtr =
        std::make_unique<ParquetTypeWithId>(
            boltType,
            std::move(children),
            curSchemaIdx,
            maxSchemaElementIdx,
            columnIdx++,
            name,
            schemaElement.type,
            logicalType_,
            convertedType,
            maxRepeat,
            maxDefine,
            isOptional,
            isRepeated,
            precision,
            scale,
            type_length);

    if (schemaElement.repetition_type ==
        thrift::FieldRepetitionType::REPEATED) {
      // Array
      children.clear();
      children.reserve(1);
      children.push_back(leafTypePtr);
      return std::make_shared<const ParquetTypeWithId>(
          TypeFactory<TypeKind::ARRAY>::create(boltType),
          std::move(children),
          curSchemaIdx,
          maxSchemaElementIdx,
          columnIdx - 1, // was already incremented for leafTypePtr
          std::move(name),
          std::nullopt,
          std::nullopt,
          std::nullopt,
          maxRepeat,
          maxDefine - 1,
          isOptional,
          isRepeated);
    }
    return leafTypePtr;
  }

  BOLT_FAIL("Unable to extract Parquet column info.")
  return nullptr;
}

void ReaderBase::collectLogicalTypes(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const std::string& path) {
  auto parquetType = std::static_pointer_cast<const ParquetTypeWithId>(type);
  if (parquetType->logicalType_.has_value()) {
    if (!path.empty()) {
      logicalTypesByPath_[path] = parquetType->logicalType_.value();
    }
  }

  if (parquetType->isLeaf()) {
    return;
  }

  for (auto& child : type->getChildren()) {
    auto parquetChild =
        std::static_pointer_cast<const ParquetTypeWithId>(child);
    auto childPath =
        path.empty() ? parquetChild->name_ : (path + "." + parquetChild->name_);
    collectLogicalTypes(child, childPath);
  }
}

void ReaderBase::collectConvertedTypes(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const std::string& path) {
  auto parquetType = std::static_pointer_cast<const ParquetTypeWithId>(type);
  if (parquetType->convertedType_.has_value()) {
    if (!path.empty()) {
      convertedTypesByPath_[path] = parquetType->convertedType_.value();
    }
  }

  if (parquetType->isLeaf()) {
    return;
  }

  for (auto& child : type->getChildren()) {
    auto parquetChild =
        std::static_pointer_cast<const ParquetTypeWithId>(child);
    auto childPath =
        path.empty() ? parquetChild->name_ : (path + "." + parquetChild->name_);
    collectConvertedTypes(child, childPath);
  }
}

TypePtr ReaderBase::convertType(
    const thrift::SchemaElement& schemaElement,
    const TypePtr& requestedType) const {
  BOLT_CHECK(schemaElement.__isset.type && schemaElement.num_children == 0);
  BOLT_CHECK(
      schemaElement.type != thrift::Type::FIXED_LEN_BYTE_ARRAY ||
          schemaElement.__isset.type_length,
      "FIXED_LEN_BYTE_ARRAY requires length to be set");

  if (schemaElement.__isset.converted_type) {
    switch (schemaElement.converted_type) {
      case thrift::ConvertedType::INT_8:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "INT8 converted type can only be set for value of thrift::Type::INT32");
        return TINYINT();

      case thrift::ConvertedType::INT_16:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "INT16 converted type can only be set for value of thrift::Type::INT32");
        return SMALLINT();

      case thrift::ConvertedType::INT_32:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "INT32 converted type can only be set for value of thrift::Type::INT32");
        return INTEGER();

      case thrift::ConvertedType::INT_64:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT64,
            "INT64 converted type can only be set for value of thrift::Type::INT64");
        return BIGINT();

      case thrift::ConvertedType::UINT_8:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "UINT_8 converted type can only be set for value of thrift::Type::INT32");
        return TINYINT();

      case thrift::ConvertedType::UINT_16:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "UINT_16 converted type can only be set for value of thrift::Type::INT32");
        return SMALLINT();

      case thrift::ConvertedType::UINT_32:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "UINT_32 converted type can only be set for value of thrift::Type::INT32");
        return INTEGER();

      case thrift::ConvertedType::UINT_64:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT64,
            "UINT_64 converted type can only be set for value of thrift::Type::INT64");
        return BIGINT();

      case thrift::ConvertedType::DATE:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT32,
            "DATE converted type can only be set for value of thrift::Type::INT32");
        return DATE();

      case thrift::ConvertedType::TIMESTAMP_MICROS:
      case thrift::ConvertedType::TIMESTAMP_MILLIS:
        BOLT_CHECK_EQ(
            schemaElement.type,
            thrift::Type::INT64,
            "TIMESTAMP_MICROS or TIMESTAMP_MILLIS converted type can only be set for value of thrift::Type::INT64");
        return TIMESTAMP();

      case thrift::ConvertedType::DECIMAL: {
        BOLT_CHECK(
            schemaElement.__isset.precision && schemaElement.__isset.scale,
            "DECIMAL requires a length and scale specifier!");
        return DECIMAL(schemaElement.precision, schemaElement.scale);
      }

      case thrift::ConvertedType::UTF8:
      case thrift::ConvertedType::JSON:
        switch (schemaElement.type) {
          case thrift::Type::BYTE_ARRAY:
          case thrift::Type::FIXED_LEN_BYTE_ARRAY:
            return VARCHAR();
          default:
            BOLT_FAIL(
                "{} converted type can only be set for thrift::Type::(FIXED_LEN_)BYTE_ARRAY.",
                thrift::to_string(schemaElement.converted_type));
        }
      case thrift::ConvertedType::MAP:
      case thrift::ConvertedType::MAP_KEY_VALUE:
      case thrift::ConvertedType::LIST:
      case thrift::ConvertedType::ENUM:
      case thrift::ConvertedType::TIME_MILLIS:
      case thrift::ConvertedType::TIME_MICROS:
      case thrift::ConvertedType::BSON:
      case thrift::ConvertedType::INTERVAL:
      default:
        BOLT_FAIL(
            "Unsupported Parquet SchemaElement converted type: {}.",
            thrift::to_string(schemaElement.converted_type));
    }
  } else {
    switch (schemaElement.type) {
      case thrift::Type::type::BOOLEAN:
        return BOOLEAN();
      case thrift::Type::type::INT32:
        return INTEGER();
      case thrift::Type::type::INT64:
        // For Int64 Timestamp in nano precision
        if (schemaElement.__isset.logicalType &&
            schemaElement.logicalType.__isset.TIMESTAMP) {
          return TIMESTAMP();
        }
        if (schemaElement.__isset.converted_type &&
            (schemaElement.converted_type ==
                 thrift::ConvertedType::TIMESTAMP_MILLIS ||
             schemaElement.converted_type ==
                 thrift::ConvertedType::TIMESTAMP_MICROS)) {
          return TIMESTAMP();
        }
        return BIGINT();
      case thrift::Type::type::INT96:
        return TIMESTAMP(); // INT96 only maps to a timestamp
      case thrift::Type::type::FLOAT:
        return REAL();
      case thrift::Type::type::DOUBLE:
        return DOUBLE();
      case thrift::Type::type::BYTE_ARRAY:
      case thrift::Type::type::FIXED_LEN_BYTE_ARRAY:
        if (requestedType && requestedType->isVarchar()) {
          return VARCHAR();
        } else {
          return VARBINARY();
        }

      default:
        BOLT_FAIL(
            "Unknown Parquet SchemaElement type: {}",
            thrift::to_string(schemaElement.type));
    }
  }
}

std::shared_ptr<const RowType> ReaderBase::createRowType(
    std::vector<std::shared_ptr<const ParquetTypeWithId::TypeWithId>> children,
    bool fileColumnNamesReadAsLowerCase) {
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  for (auto& child : children) {
    auto childName =
        std::static_pointer_cast<const ParquetTypeWithId>(child)->name_;
    if (fileColumnNamesReadAsLowerCase) {
      folly::toLowerAscii(childName);
    }
    childNames.push_back(std::move(childName));
    childTypes.push_back(child->type());
  }
  return TypeFactory<TypeKind::ROW>::create(
      std::move(childNames), std::move(childTypes));
}

std::shared_ptr<const dwio::common::TypeWithId> ReaderBase::createTypeWithId(
    const std::shared_ptr<const dwio::common::TypeWithId>& inputType,
    const RowTypePtr& rowTypePtr,
    bool fileColumnNamesReadAsLowerCase) {
  if (!fileColumnNamesReadAsLowerCase) {
    return inputType;
  }
  std::vector<std::string> names;
  names.reserve(rowTypePtr->names().size());
  std::vector<TypePtr> types = rowTypePtr->children();
  for (const auto& name : rowTypePtr->names()) {
    std::string childName = name;
    folly::toLowerAscii(childName);
    names.emplace_back(childName);
  }
  auto convertedType =
      TypeFactory<TypeKind::ROW>::create(std::move(names), std::move(types));

  auto children = inputType->getChildren();
  return std::make_shared<const dwio::common::TypeWithId>(
      convertedType,
      std::move(children),
      inputType->id(),
      inputType->maxId(),
      inputType->column());
}

void ReaderBase::scheduleRowGroups(
    const std::vector<uint32_t>& rowGroupIds,
    int32_t currentGroup,
    StructColumnReader& reader) {
  // clear old RowGroup
  if (currentGroup >= 1) {
    inputs_.erase(rowGroupIds[currentGroup - 1]);
  }
  // load current RowGroup and prefetch new RowGroup
  auto numRowGroupsToLoad = std::min(
      options_.prefetchRowGroups() + 1,
      static_cast<int64_t>(rowGroupIds.size() - currentGroup));
  for (auto i = 0; i < numRowGroupsToLoad; i++) {
    auto thisGroup = rowGroupIds[currentGroup + i];
    if (!inputs_[thisGroup]) {
      inputs_[thisGroup] = reader.loadRowGroup(thisGroup, input_);
    }
  }
}

namespace {
int32_t parquetTypeToBoltBytes(thrift::Type::type type) {
  switch (type) {
    case thrift::Type::INT32:
    case thrift::Type::FLOAT:
      return 4;
    case thrift::Type::INT64:
    case thrift::Type::DOUBLE:
      return 8;
    case thrift::Type::INT96:
      // corresponding to timestamp of the bolt, 16 bytes
    case thrift::Type::FIXED_LEN_BYTE_ARRAY:
      // The length of FIXED_LEN_BYTE_ARRAY is recorded in the schema in the
      // parquet metadata, which is mainly used for 16 bytes LongDecimalType in
      // bolt
      return 16;
    case thrift::Type::BYTE_ARRAY:
      return sizeof(StringView);
    default:
      BOLT_FAIL("Type does not have a byte width {}", type);
  }
}
} // namespace

int64_t ReaderBase::estimatedRowGroupBytesInMemory(
    int32_t rowGroupIndex,
    const dwio::common::TypeWithId& type) const {
  if (type.column() != ParquetTypeWithId::kNonLeaf) {
    const auto& metaData = fileMetaData_->row_groups[rowGroupIndex]
                               .columns[type.column()]
                               .meta_data;
    if (metaData.type == thrift::Type::BOOLEAN) {
      return metaData.total_uncompressed_size;
    } else {
      // Leaf type bytes multiply by rows of leaf type
      auto bytes = parquetTypeToBoltBytes(metaData.type) * metaData.num_values;
      if (metaData.type == thrift::Type::BYTE_ARRAY) {
        bytes += metaData.total_uncompressed_size;
      }
      return bytes;
    }
  }
  int64_t sum = 0;
  for (auto child : type.getChildren()) {
    sum += estimatedRowGroupBytesInMemory(rowGroupIndex, *child);
  }
  return sum;
}

bool ReaderBase::isRowGroupBuffered(int32_t rowGroupIndex) const {
  return inputs_.count(rowGroupIndex) != 0;
}

uint32_t ReaderBase::parseEncryptedFooter(
    const char* cryptoMetadataBuffer,
    uint32_t footerLen) {
  auto cryptoMetadataLen = footerLen;
  thrift::FileCryptoMetaData fileCryptoMetadata;
  thrift::ThriftDeserializer deserializer;
  deserializer.DeserializeUnencryptedMessage(
      reinterpret_cast<const uint8_t*>(cryptoMetadataBuffer),
      &cryptoMetadataLen,
      &fileCryptoMetadata);

  // Add KMS metadata to retrieve the key. Since the implementation of the
  // KMS client varies among different KMS systems, we leave a base KMS
  // client class here. You can implement your own KMS client.

  cryptoFactory_ = std::make_shared<::parquet::encryption::CryptoFactory>();

  std::shared_ptr<KmsClientFactory> kmsClientFactory =
      std::make_shared<KmsClientFactory>();

  std::shared_ptr<KmsConfBuilder> kmsConfBuilder =
      std::make_shared<KmsConfBuilder>(fileCryptoMetadata.key_metadata);

  ::parquet::encryption::KmsConnectionConfig kmsConf = kmsConfBuilder->build();
  ::parquet::encryption::DecryptionConfiguration decryptionConf =
      ::parquet::encryption::DecryptionConfiguration();

  cryptoFactory_->RegisterKmsClientFactory(std::move(kmsClientFactory));

  fileDecryptionProperties_ =
      cryptoFactory_->GetFileDecryptionProperties(kmsConf, decryptionConf);

  thrift::EncryptionAlgorithm encryption =
      fileCryptoMetadata.encryption_algorithm;
  ::parquet::EncryptionAlgorithm algo = FromThrift(encryption);
  std::string fileAad = HandleAadPrefix(fileDecryptionProperties_, algo);
  fileDecryptor_ = std::make_shared<InternalFileDecryptor>(
      fileDecryptionProperties_.get(),
      fileAad,
      algo.algorithm == ::parquet::ParquetCipher::type::AES_GCM_V1
          ? ParquetCipher::type::AES_GCM_V1
          : ParquetCipher::type::AES_GCM_CTR_V1,
      fileCryptoMetadata.key_metadata,
      &getMemoryPool());
  return cryptoMetadataLen;
}

std::string ReaderBase::HandleAadPrefix(
    const std::shared_ptr<::parquet::FileDecryptionProperties>&
        fileDecryptionProperties,
    ::parquet::EncryptionAlgorithm& algo) {
  std::string aadPrefixInProperties = fileDecryptionProperties->aad_prefix();
  std::string aadPrefix = aadPrefixInProperties;
  bool fileHasAadPrefix = algo.aad.aad_prefix.empty() ? false : true;
  std::string aadPrefixInFile = algo.aad.aad_prefix;
  if (algo.aad.supply_aad_prefix && aadPrefixInProperties.empty()) {
    BOLT_FAIL(
        "AAD prefix used for file encryption, but not stored in file and not supplied in decryption properties");
  }
  if (fileHasAadPrefix) {
    if (!aadPrefixInProperties.empty()) {
      if (aadPrefixInProperties.compare(aadPrefixInFile) != 0) {
        BOLT_FAIL("AAD Prefix in file and in properties is not the same");
      }
    }
    aadPrefix = aadPrefixInFile;
    const std::shared_ptr<::parquet::AADPrefixVerifier>& aadPrefixVerifier =
        fileDecryptionProperties->aad_prefix_verifier();
    if (aadPrefixVerifier != nullptr) {
      aadPrefixVerifier->Verify(aadPrefix);
    }
  } else {
    if (!algo.aad.supply_aad_prefix && !aadPrefixInProperties.empty()) {
      BOLT_FAIL(
          "AAD Prefix set in decryption properties, but was not used for file encryption");
    }
    const std::shared_ptr<::parquet::AADPrefixVerifier>& aadPrefixVerifier =
        fileDecryptionProperties->aad_prefix_verifier();
    if (aadPrefixVerifier != nullptr) {
      BOLT_FAIL("AAD Prefix Verifier is set, but AAD Prefix not found in file");
    }
  }
  return aadPrefix + algo.aad.aad_file_unique;
}

// Parse plain footers in Encrypted Parquet files.
void ReaderBase::parsePlainFooter() {
  // Providing decryption properties in plaintext footer mode is not
  // mandatory, for example when reading by legacy reader.
  auto* file_decryption_properties = fileDecryptionProperties_.get();
  if (file_decryption_properties != nullptr) {
    thrift::EncryptionAlgorithm& thriftAlgo =
        fileMetaData_->encryption_algorithm;
    ::parquet::EncryptionAlgorithm algo;
    ParquetCipher::type algorithmType = ParquetCipher::type::AES_GCM_V1;
    // from thrift algo to arrow parquet EncryptionAlgorithm
    if (thriftAlgo.__isset.AES_GCM_V1) {
      algo.algorithm = ::parquet::ParquetCipher::type::AES_GCM_V1;
      algo.aad.aad_prefix = thriftAlgo.AES_GCM_V1.aad_prefix;
      algo.aad.aad_file_unique = thriftAlgo.AES_GCM_V1.aad_file_unique;
      algo.aad.supply_aad_prefix = thriftAlgo.AES_GCM_V1.supply_aad_prefix;
    } else if (thriftAlgo.__isset.AES_GCM_CTR_V1) {
      algorithmType = ParquetCipher::type::AES_GCM_CTR_V1;
      algo.algorithm = ::parquet::ParquetCipher::type::AES_GCM_CTR_V1;
      algo.aad.aad_prefix = thriftAlgo.AES_GCM_CTR_V1.aad_prefix;
      algo.aad.aad_file_unique = thriftAlgo.AES_GCM_CTR_V1.aad_file_unique;
      algo.aad.supply_aad_prefix = thriftAlgo.AES_GCM_CTR_V1.supply_aad_prefix;
    } else {
      BOLT_FAIL("ParquetCipher::type not specified");
    }
    // Handle AAD prefix
    std::string file_aad = HandleAadPrefix(fileDecryptionProperties_, algo);
    fileDecryptor_ = std::make_shared<InternalFileDecryptor>(
        file_decryption_properties,
        file_aad,
        algorithmType,
        fileMetaData_->footer_signing_key_metadata,
        &pool_);
    // TODO : check_plaintext_footer_integrity
  }
}

namespace {
struct ParquetStatsContext : dwio::common::StatsContext {};
} // namespace

class ParquetRowReader::Impl {
 public:
  Impl(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options)
      : pool_(readerBase->getMemoryPool()),
        readerBase_(readerBase),
        options_(options),
        rowGroups_(readerBase_->thriftFileMetaData().row_groups),
        nextRowGroupIdsIdx_(0),
        currentRowGroupPtr_(nullptr),
        rowsInCurrentRowGroup_(0),
        currentRowInGroup_(0),
        schemaHelper_(readerBase_->thriftFileMetaData().schema) {
    // Validate the requested type is compatible with what's in the file
    std::function<std::string()> createExceptionContext = [&]() {
      std::string exceptionMessageContext = fmt::format(
          "The schema loaded in the reader does not match the schema in the file footer."
          "Input Name: {},\n"
          "File Footer Schema (without partition columns): {},\n"
          "Input Table Schema (with partition columns): {}\n",
          readerBase_->bufferedInput().getReadFile()->getName(),
          readerBase_->schema()->toString(),
          requestedType_->toString());
      return exceptionMessageContext;
    };

    if (rowGroups_.empty()) {
      return; // TODO
    }
    ParquetParams params(
        pool_,
        columnReaderStats_,
        readerBase_->thriftFileMetaData(),
        options_.timestampPrecision(),
        readerBase->fileDecryptor(),
        schemaHelper_,
        options_.isDictionaryFilterEnabled(),
        options_.getDecodeRepDefPageCount(),
        options_.getParquetRepDefMemoryLimit());

    if (auto selector = options_.getSelector()) {
      requestedType_ = selector->getSchema();
    } else {
      requestedType_ = readerBase_->schema();
    }

    auto requestedTypeWithId = ReaderBase::createTypeWithId(
        dwio::common::TypeWithId::create(requestedType_),
        asRowType(requestedType_),
        readerBase_->isFileColumnNamesReadAsLowerCase());
    columnReaderOptions_ =
        dwio::common::makeColumnReaderOptions(readerBase_->options());
    columnReader_ = ParquetColumnReader::build(
        columnReaderOptions_,
        requestedTypeWithId,
        readerBase_->schemaWithId(), // Id is schema id
        params,
        *options_.getScanSpec(),
        pool_);

    // Annotate scan spec with logical type names before filtering row groups.
    if (auto scanSpecPtr = options_.getScanSpec()) {
      const auto& ltMap = readerBase_->schemaLogicalTypes();
      const auto& ctMap = readerBase_->schemaConvertedTypes();
      auto& scanSpec = *scanSpecPtr;
      auto annotate = [&](common::ScanSpec& spec,
                          const std::string& basePath,
                          const dwio::common::TypeWithId& twi,
                          auto&& selfRef) -> void {
        std::string path = basePath;
        if (!spec.fieldName().empty()) {
          if (basePath.empty() || basePath == "root" || basePath == "<root>" ||
              basePath == "hive_schema") {
            path = spec.fieldName();
          } else {
            path = basePath + "." + spec.fieldName();
          }
        }
        auto it = ltMap.find(path);
        if (it != ltMap.end()) {
          // Convert thrift::LogicalType to a readable identifier
          const auto& lt = it->second;
          std::string name;
          if (lt.__isset.INTEGER)
            name = "INTEGER";
          else if (lt.__isset.DECIMAL)
            name = "DECIMAL";
          else if (lt.__isset.STRING)
            name = "STRING";
          else if (lt.__isset.DATE)
            name = "DATE";
          else if (lt.__isset.TIME)
            name = "TIME";
          else if (lt.__isset.TIMESTAMP)
            name = "TIMESTAMP";
          else if (lt.__isset.UUID)
            name = "UUID";
          else if (lt.__isset.ENUM)
            name = "ENUM";
          else if (lt.__isset.UNKNOWN)
            name = "UNKNOWN";
          else if (lt.__isset.JSON)
            name = "JSON";
          else if (lt.__isset.BSON)
            name = "BSON";
          else if (lt.__isset.LIST)
            name = "LIST";
          else if (lt.__isset.MAP)
            name = "MAP";
          if (!spec.logicalTypeName().empty() &&
              spec.logicalTypeName() != name) {
            BOLT_FAIL(fmt::format(
                "LogicalType mismatch for path {}: scanSpec={}, schema={}",
                path,
                spec.logicalTypeName(),
                name));
          }
          spec.setLogicalTypeName(name);
        }
        auto ctIt = ctMap.find(path);
        if (ctIt != ctMap.end()) {
          const auto name = thrift::to_string(ctIt->second);
          BOLT_CHECK(
              spec.convertedTypeName().empty() ||
                  spec.convertedTypeName() == name,
              fmt::format(
                  "ConvertedType mismatch for path {}: scanSpec={}, schema={}",
                  path,
                  spec.convertedTypeName(),
                  name));
          spec.setConvertedTypeName(name);
        }
        VLOG(2) << "Annotate path=" << path << " field=" << spec.fieldName()
                << " kind=" << static_cast<int>(twi.type()->kind())
                << " assignedLogicalType=" << spec.logicalTypeName();
        for (auto* child : spec.stableChildren()) {
          // Best-effort: find child by name in TypeWithId if available
          std::shared_ptr<const dwio::common::TypeWithId> childTwi;
          if (twi.type()->kind() == TypeKind::ROW &&
              twi.containsChild(child->fieldName())) {
            childTwi = twi.childByName(child->fieldName());
          } else {
            childTwi = nullptr;
          }
          VLOG(2) << "Annotate child field=" << child->fieldName()
                  << " childTwi=" << (childTwi ? "found" : "missing")
                  << " basePath=" << path;
          if (childTwi) {
            selfRef(*child, path, *childTwi, selfRef);
          }
        }
      };
      annotate(scanSpec, "", *readerBase_->schemaWithId(), annotate);
    }

    filterRowGroups();
    if (!rowGroupIds_.empty()) {
      // schedule prefetch of first row group right after reading the metadata.
      // This is usually on a split preload thread before the split goes to
      // table scan.
      advanceToNextRowGroup();
    }
  }

  void filterRowGroups() {
    rowGroupIds_.reserve(rowGroups_.size());
    firstRowOfRowGroup_.reserve(rowGroups_.size());

    ParquetData::FilterRowGroupsResult res;
    columnReader_->filterRowGroups(
        0, ParquetStatsContext(), res, readerBase_->bufferedInput());
    if (auto& metadataFilter = options_.getMetadataFilter()) {
      metadataFilter->eval(res.metadataFilterResults, res.filterResult);
    }

    uint64_t rowNumber = 0;
    for (auto i = 0; i < rowGroups_.size(); i++) {
      BOLT_CHECK_GT(rowGroups_[i].columns.size(), 0);
      auto fileOffset = rowGroups_[i].__isset.file_offset
          ? rowGroups_[i].file_offset
          : rowGroups_[i].columns[0].meta_data.__isset.dictionary_page_offset

          ? rowGroups_[i].columns[0].meta_data.dictionary_page_offset
          : rowGroups_[i].columns[0].meta_data.data_page_offset;
      BOLT_CHECK_GT(fileOffset, 0);
      auto rowGroupInRange =
          (fileOffset >= options_.getOffset() &&
           fileOffset < options_.getLimit());

      auto isExcluded =
          (i < res.totalCount && bits::isBitSet(res.filterResult.data(), i));
      auto isEmpty = rowGroups_[i].num_rows == 0;

      // Add a row group to read if it is within range and not empty and not in
      // the excluded list.
      if (rowGroupInRange && !isExcluded && !isEmpty) {
        rowGroupIds_.push_back(i);
        firstRowOfRowGroup_.push_back(rowNumber);
      }
      rowNumber += rowGroups_[i].num_rows;
    }

    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Filtered row groups: " << rowGroupIds_.size();
      VLOG(1) << "Total row groups: " << rowGroups_.size();
      VLOG(1) << "Total rows: " << rowNumber;
    }
  }

  int64_t nextRowNumber() {
    if (currentRowInGroup_ >= rowsInCurrentRowGroup_ &&
        !advanceToNextRowGroup()) {
      return kAtEnd;
    }
    return firstRowOfRowGroup_[nextRowGroupIdsIdx_ - 1] + currentRowInGroup_;
  }

  int64_t nextReadSize(uint64_t size) {
    BOLT_CHECK_GT(size, 0);
    if (nextRowNumber() == kAtEnd) {
      return kAtEnd;
    }
    return std::min(size, rowsInCurrentRowGroup_ - currentRowInGroup_);
  }

  uint64_t skip(uint64_t skipSize) {
    auto rowsToSkip = nextReadSize(skipSize);
    if (rowsToSkip == kAtEnd) {
      return 0;
    }

    BOLT_DCHECK_GT(rowsToSkip, 0);
    columnReader_->setReadOffset(columnReader_->readOffset() + rowsToSkip);
    currentRowInGroup_ += rowsToSkip;
    return rowsToSkip;
  }

  uint64_t next(
      uint64_t size,
      bolt::VectorPtr& result,
      const dwio::common::Mutation* mutation) {
    BOLT_DCHECK(!options_.getAppendRowNumberColumn());
    auto rowsToRead = nextReadSize(size);
    if (rowsToRead == kAtEnd) {
      return 0;
    }
    BOLT_DCHECK_GT(rowsToRead, 0);
    columnReader_->next(rowsToRead, result, mutation);
    currentRowInGroup_ += rowsToRead;
    return rowsToRead;
  }

  std::optional<size_t> estimatedRowSize() const {
    auto index =
        nextRowGroupIdsIdx_ < 1 ? 0 : rowGroupIds_[nextRowGroupIdsIdx_ - 1];
    return readerBase_->estimatedRowGroupBytesInMemory(
               index,
               *dynamic_cast<bytedance::bolt::parquet::StructColumnReader*>(
                    columnReader_.get())
                    ->requestedTypeWithId()) /
        rowGroups_[index].num_rows;
  }

  void updateRuntimeStats(dwio::common::RuntimeStatistics& stats) const {
    stats.skippedStrides += rowGroups_.size() - rowGroupIds_.size();
    stats.processedStrides += rowGroupIds_.size();
  }

  void resetFilterCaches() {
    columnReader_->resetFilterCaches();
  }

  bool isRowGroupBuffered(int32_t rowGroupIndex) const {
    return readerBase_->isRowGroupBuffered(rowGroupIndex);
  }

 private:
  bool advanceToNextRowGroup() {
    if (nextRowGroupIdsIdx_ == rowGroupIds_.size()) {
      return false;
    }

    auto nextRowGroupIndex = rowGroupIds_[nextRowGroupIdsIdx_];
    readerBase_->scheduleRowGroups(
        rowGroupIds_,
        nextRowGroupIdsIdx_,
        static_cast<StructColumnReader&>(*columnReader_));
    currentRowGroupPtr_ = &rowGroups_[rowGroupIds_[nextRowGroupIdsIdx_]];
    rowsInCurrentRowGroup_ = currentRowGroupPtr_->num_rows;
    currentRowInGroup_ = 0;
    nextRowGroupIdsIdx_++;
    columnReader_->seekToRowGroup(nextRowGroupIndex);
    return true;
  }

  memory::MemoryPool& pool_;
  const std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::RowReaderOptions options_;

  // All row groups from file metadata.
  const std::vector<thrift::RowGroup>& rowGroups_;
  // Indices of row groups where stats match filters.
  std::vector<uint32_t> rowGroupIds_;
  std::vector<uint64_t> firstRowOfRowGroup_;
  uint32_t nextRowGroupIdsIdx_;
  const thrift::RowGroup* FOLLY_NULLABLE currentRowGroupPtr_{nullptr};
  uint64_t rowsInCurrentRowGroup_;
  uint64_t currentRowInGroup_;

  std::unique_ptr<dwio::common::SelectiveColumnReader> columnReader_;

  RowTypePtr requestedType_;
  dwio::common::ColumnReaderOptions columnReaderOptions_;

  dwio::common::ColumnReaderStatistics columnReaderStats_;
  SchemaHelper schemaHelper_;
};

ParquetRowReader::ParquetRowReader(
    const std::shared_ptr<ReaderBase>& readerBase,
    const dwio::common::RowReaderOptions& options) {
  impl_ = std::make_unique<ParquetRowReader::Impl>(readerBase, options);
}

void ParquetRowReader::filterRowGroups() {
  impl_->filterRowGroups();
}

int64_t ParquetRowReader::nextRowNumber() {
  return impl_->nextRowNumber();
}

int64_t ParquetRowReader::nextReadSize(uint64_t size) {
  return impl_->nextReadSize(size);
}

uint64_t ParquetRowReader::next(
    uint64_t size,
    bolt::VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  return impl_->next(size, result, mutation);
}

uint64_t ParquetRowReader::skip(uint64_t skipSize) {
  return impl_->skip(skipSize);
}

void ParquetRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  impl_->updateRuntimeStats(stats);
}

void ParquetRowReader::resetFilterCaches() {
  impl_->resetFilterCaches();
}

bool ParquetRowReader::isRowGroupBuffered(int32_t rowGroupIndex) const {
  return impl_->isRowGroupBuffered(rowGroupIndex);
}

std::optional<size_t> ParquetRowReader::estimatedRowSize() const {
  return impl_->estimatedRowSize();
}

ParquetReader::ParquetReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : readerBase_(std::make_shared<ReaderBase>(std::move(input), options)) {}

std::optional<uint64_t> ParquetReader::numberOfRows() const {
  return readerBase_->thriftFileMetaData().num_rows;
}

const bolt::RowTypePtr& ParquetReader::rowType() const {
  return readerBase_->schema();
}

const std::shared_ptr<const dwio::common::TypeWithId>&
ParquetReader::typeWithId() const {
  return readerBase_->schemaWithId();
}

std::unique_ptr<dwio::common::RowReader> ParquetReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<ParquetRowReader>(readerBase_, options);
}

FileMetaDataPtr ParquetReader::fileMetaData() const {
  return readerBase_->fileMetaData();
}

const std::shared_ptr<const RowType> ParquetReader::fileSchema() const {
  return readerBase_->schema();
}

} // namespace bytedance::bolt::parquet

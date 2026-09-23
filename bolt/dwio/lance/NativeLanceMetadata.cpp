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

#include "bolt/dwio/lance/NativeLanceMetadata.h"

#include <algorithm>
#include <array>
#include <limits>
#include <numeric>
#include <string_view>
#include <unordered_map>

#include <folly/lang/Bits.h>
#include <google/protobuf/any.pb.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_0.pb.h"
#include "bolt/dwio/lance/proto/lance_file.pb.h"

namespace bytedance::bolt::lance::reader {
namespace {

::lance::encodings::ArrayEncoding parsePageEncoding(
    const ::lance::file::v2::ColumnMetadata::Page& page,
    uint32_t columnIndex,
    int32_t pageIndex) {
  BOLT_CHECK(
      page.has_encoding(),
      "Lance column {} page {} has no encoding",
      columnIndex,
      pageIndex);
  BOLT_CHECK_EQ(
      page.encoding().location_case(),
      ::lance::file::v2::Encoding::kDirect,
      "Native Lance reader requires direct page encodings (column {}, page {})",
      columnIndex,
      pageIndex);
  google::protobuf::Any envelope;
  BOLT_CHECK(
      envelope.ParseFromString(page.encoding().direct().encoding()),
      "Invalid Lance page encoding envelope (column {}, page {})",
      columnIndex,
      pageIndex);
  BOLT_CHECK_EQ(
      envelope.type_url(),
      "/lance.encodings.ArrayEncoding",
      "Unsupported Lance page encoding type '{}' (column {}, page {})",
      envelope.type_url(),
      columnIndex,
      pageIndex);
  ::lance::encodings::ArrayEncoding encoding;
  BOLT_CHECK(
      encoding.ParseFromString(envelope.value()),
      "Invalid Lance array encoding (column {}, page {})",
      columnIndex,
      pageIndex);
  return encoding;
}

::lance::encodings21::PageLayout parsePageLayout(
    const ::lance::file::v2::ColumnMetadata::Page& page,
    uint32_t columnIndex,
    int32_t pageIndex) {
  BOLT_CHECK(
      page.has_encoding(),
      "Lance column {} page {} has no encoding",
      columnIndex,
      pageIndex);
  BOLT_CHECK_EQ(
      page.encoding().location_case(),
      ::lance::file::v2::Encoding::kDirect,
      "Native Lance reader requires direct page encodings (column {}, page {})",
      columnIndex,
      pageIndex);
  google::protobuf::Any envelope;
  BOLT_CHECK(
      envelope.ParseFromString(page.encoding().direct().encoding()),
      "Invalid Lance page layout envelope (column {}, page {})",
      columnIndex,
      pageIndex);
  BOLT_CHECK_EQ(
      envelope.type_url(),
      "/lance.encodings21.PageLayout",
      "Unsupported Lance page layout type '{}' (column {}, page {})",
      envelope.type_url(),
      columnIndex,
      pageIndex);
  ::lance::encodings21::PageLayout layout;
  BOLT_CHECK(
      layout.ParseFromString(envelope.value()),
      "Invalid Lance page layout (column {}, page {})",
      columnIndex,
      pageIndex);
  return layout;
}

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
}

TypePtr primitiveType(std::string_view logicalType) {
  if (logicalType.rfind("dict:", 0) == 0) {
    return primitiveType(nativeLanceDictionaryValueLogicalType(logicalType));
  }
  if (logicalType == "null") {
    return UNKNOWN();
  }
  if (logicalType == "bool") {
    return BOOLEAN();
  }
  if (logicalType == "int8") {
    return TINYINT();
  }
  if (logicalType == "uint8") {
    return SMALLINT();
  }
  if (logicalType == "int16") {
    return SMALLINT();
  }
  if (logicalType == "uint16") {
    return INTEGER();
  }
  if (logicalType == "int32") {
    return INTEGER();
  }
  if (logicalType == "uint32") {
    return BIGINT();
  }
  if (logicalType == "int64") {
    return BIGINT();
  }
  if (logicalType == "uint64") {
    return HUGEINT();
  }
  if (logicalType == "halffloat") {
    return REAL();
  }
  if (logicalType == "float") {
    return REAL();
  }
  if (logicalType == "double") {
    return DOUBLE();
  }
  if (logicalType == "string" || logicalType == "large_string") {
    return VARCHAR();
  }
  if (logicalType == "binary" || logicalType == "large_binary" ||
      logicalType == "json") {
    return VARBINARY();
  }
  if (logicalType.rfind("fixed_size_binary:", 0) == 0) {
    const auto byteWidth = std::stoi(std::string(
        logicalType.substr(std::string_view("fixed_size_binary:").size())));
    BOLT_CHECK_GT(byteWidth, 0, "Invalid Lance fixed-size binary width");
    return VARBINARY();
  }
  if (logicalType == "lance.bfloat16") {
    // Bolt has no bfloat16 scalar. Preserve the two-byte Arrow extension
    // storage exactly, matching the type the existing Arrow bridge can carry.
    return VARBINARY();
  }
  if (logicalType.rfind("fixed_size_list:", 0) == 0) {
    constexpr auto kPrefix = std::string_view("fixed_size_list:");
    const auto dimensionStart = logicalType.rfind(':');
    BOLT_CHECK_GT(
        dimensionStart, kPrefix.size(), "Invalid Lance fixed-size list type");
    const auto dimension =
        std::stoi(std::string(logicalType.substr(dimensionStart + 1)));
    BOLT_CHECK_GT(dimension, 0, "Invalid Lance fixed-size list dimension");
    return ARRAY(primitiveType(
        logicalType.substr(kPrefix.size(), dimensionStart - kPrefix.size())));
  }
  if (logicalType == "date32:day") {
    return DATE();
  }
  if (logicalType == "date64:ms") {
    return DATE();
  }
  if (logicalType == "time32:s" || logicalType == "time32:ms" ||
      logicalType == "time64:us" || logicalType == "time64:ns") {
    // Bolt has no TIME scalar. Preserve the physical unit in a signed integer.
    return BIGINT();
  }
  if (logicalType == "duration:s" || logicalType == "duration:ms" ||
      logicalType == "duration:us" || logicalType == "duration:ns") {
    return INTERVAL_DAY_TIME();
  }
  if (logicalType.rfind("timestamp:s:", 0) == 0 ||
      logicalType.rfind("timestamp:ms:", 0) == 0 ||
      logicalType.rfind("timestamp:us:", 0) == 0 ||
      logicalType.rfind("timestamp:ns:", 0) == 0) {
    return TIMESTAMP();
  }
  if (logicalType.rfind("decimal:128:", 0) == 0) {
    const auto precisionStart = std::string_view("decimal:128:").size();
    const auto scaleStart = logicalType.find(':', precisionStart);
    BOLT_CHECK_NE(
        scaleStart,
        std::string_view::npos,
        "Invalid Lance decimal logical type: {}",
        logicalType);
    const auto precision = std::stoi(std::string(
        logicalType.substr(precisionStart, scaleStart - precisionStart)));
    const auto scale =
        std::stoi(std::string(logicalType.substr(scaleStart + 1)));
    BOLT_CHECK_LE(
        precision, 38, "Unsupported Lance decimal precision: {}", precision);
    BOLT_CHECK_GE(
        scale, 0, "Negative Lance decimal scales are not supported: {}", scale);
    BOLT_CHECK_LE(
        scale,
        precision,
        "Lance decimal scale exceeds precision: {}",
        logicalType);
    return DECIMAL(precision, scale);
  }
  if (logicalType.rfind("decimal:256:", 0) == 0) {
    BOLT_UNSUPPORTED(
        "Lance Decimal256 cannot be represented losslessly by Bolt: {}",
        logicalType);
  }
  if (logicalType == "union" || logicalType.rfind("union:", 0) == 0) {
    BOLT_UNSUPPORTED(
        "Lance Union cannot be represented losslessly by Bolt: {}",
        logicalType);
  }
  if (logicalType == "run_end_encoded" ||
      logicalType.rfind("run_end_encoded:", 0) == 0) {
    BOLT_UNSUPPORTED(
        "Lance RunEndEncoded cannot be represented losslessly by Bolt: {}",
        logicalType);
  }
  if (logicalType == "list_view" || logicalType.rfind("list_view:", 0) == 0 ||
      logicalType == "large_list_view" ||
      logicalType.rfind("large_list_view:", 0) == 0) {
    BOLT_UNSUPPORTED(
        "Lance ListView cannot be represented without changing offset "
        "semantics: {}",
        logicalType);
  }
  if (logicalType == "interval" || logicalType.rfind("interval:", 0) == 0) {
    BOLT_UNSUPPORTED(
        "Lance Interval has no stable file logical type contract: {}",
        logicalType);
  }
  BOLT_UNSUPPORTED("Unsupported Lance logical type: {}", logicalType);
}

bool isBlobField(const ::lance::file::Field& field) {
  return field.metadata().contains("lance-encoding:blob") ||
      field.extension_name() == "lance.blob" ||
      field.extension_name() == "lance.blob.v2";
}

bool isBlobV2Field(const ::lance::file::Field& field) {
  return field.logical_type() == "struct" && isBlobField(field) &&
      (field.metadata().contains("lance-encoding:packed") ||
       field.extension_name() == "lance.blob.v2");
}

std::string_view extensionName(const ::lance::file::Field& field) {
  const auto metadataExtension = field.metadata().find("ARROW:extension:name");
  const auto extension = !field.extension_name().empty()
      ? std::string_view(field.extension_name())
      : metadataExtension == field.metadata().end()
      ? std::string_view{}
      : std::string_view(metadataExtension->second);
  BOLT_CHECK(
      field.extension_name().empty() ||
          metadataExtension == field.metadata().end() ||
          field.extension_name() == metadataExtension->second,
      "Conflicting Lance Arrow extension names '{}' and '{}' on field '{}'",
      field.extension_name(),
      metadataExtension == field.metadata().end() ? std::string{}
                                                  : metadataExtension->second,
      field.name());
  return extension;
}

std::string_view extensionMetadata(const ::lance::file::Field& field) {
  const auto metadata = field.metadata().find("ARROW:extension:metadata");
  return metadata == field.metadata().end()
      ? std::string_view{}
      : std::string_view(metadata->second);
}

TypePtr adaptSemanticType(
    const ::lance::file::Field& field,
    TypePtr storageType,
    const std::shared_ptr<const NativeLanceTypeAdapter>& adapter) {
  const auto extension = extensionName(field);
  if (extension == "lance.json" || extension == "arrow.json") {
    BOLT_CHECK_EQ(
        field.logical_type(),
        "json",
        "Lance JSON extension '{}' has unexpected storage logical type '{}'",
        extension,
        field.logical_type());
  } else if (extension == "lance.bfloat16") {
    BOLT_CHECK_EQ(
        field.logical_type(),
        "fixed_size_binary:2",
        "Lance bfloat16 extension has unexpected storage logical type '{}'",
        field.logical_type());
  } else if (extension == "lance.blob" || extension == "lance.blob.v2") {
    return storageType;
  }
  auto logicalType = std::string_view(field.logical_type());
  if (logicalType.rfind("dict:", 0) == 0) {
    logicalType = nativeLanceDictionaryValueLogicalType(logicalType);
  }
  if (logicalType.rfind("fixed_size_list:", 0) == 0) {
    constexpr auto kPrefix = std::string_view("fixed_size_list:");
    const auto dimension = logicalType.rfind(':');
    BOLT_CHECK_GT(dimension, kPrefix.size());
    BOLT_CHECK_EQ(storageType->kind(), TypeKind::ARRAY);
    ::lance::file::Field childField;
    childField.set_name(field.name());
    childField.set_logical_type(std::string(
        logicalType.substr(kPrefix.size(), dimension - kPrefix.size())));
    return ARRAY(
        adaptSemanticType(childField, storageType->childAt(0), adapter));
  }
  if (adapter != nullptr) {
    if (auto adapted = adapter->resolve(
            {.fieldName = field.name(),
             .logicalType = logicalType,
             .extensionName = extension,
             .extensionMetadata = extensionMetadata(field),
             .storageType = storageType})) {
      BOLT_CHECK_NOT_NULL(*adapted);
      BOLT_CHECK_EQ(
          (*adapted)->kind(),
          storageType->kind(),
          "Lance semantic type adapter changed physical kind for field '{}'",
          field.name());
      return *adapted;
    }
  }
  const auto defaultAdapter = defaultNativeLanceTypeAdapter();
  if (adapter != defaultAdapter) {
    if (auto adapted = defaultAdapter->resolve(
            {.fieldName = field.name(),
             .logicalType = logicalType,
             .extensionName = extension,
             .extensionMetadata = extensionMetadata(field),
             .storageType = storageType})) {
      BOLT_CHECK_NOT_NULL(*adapted);
      BOLT_CHECK_EQ(
          (*adapted)->kind(),
          storageType->kind(),
          "Default Lance semantic type adapter changed physical kind for field '{}'",
          field.name());
      return *adapted;
    }
  }
  if (!extension.empty()) {
    BOLT_UNSUPPORTED(
        "Unsupported Lance Arrow extension '{}' on field '{}'; refusing to "
        "silently expose only its storage type '{}'",
        extension,
        field.name(),
        field.logical_type());
  }
  return storageType;
}

std::string_view effectiveLogicalType(const ::lance::file::Field& field) {
  return isBlobV2Field(field) ? std::string_view("lance.blob.v2")
                              : std::string_view(field.logical_type());
}

struct SchemaTree {
  std::vector<const ::lance::file::Field*> roots;
  std::unordered_map<int32_t, std::vector<const ::lance::file::Field*>>
      childrenByParent;
};

SchemaTree buildSchemaTree(const ::lance::file::Schema& schema) {
  constexpr std::array<std::pair<std::string_view, std::string_view>, 5>
      kBlobV2DescriptorFields{{
          {"kind", "uint8"},
          {"position", "uint64"},
          {"size", "uint64"},
          {"blob_id", "uint32"},
          {"blob_uri", "string"},
      }};

  SchemaTree result;
  std::unordered_map<int32_t, const ::lance::file::Field*> fieldsById;
  for (int32_t index = 0; index < schema.fields_size(); ++index) {
    const auto& field = schema.fields(index);
    BOLT_CHECK(
        fieldsById.emplace(field.id(), &field).second,
        "Duplicate Lance field id: {}",
        field.id());
    if (field.parent_id() < 0) {
      result.roots.push_back(&field);
    } else {
      result.childrenByParent[field.parent_id()].push_back(&field);
    }

    if (!isBlobV2Field(field)) {
      continue;
    }
    BOLT_CHECK_LE(
        index + static_cast<int32_t>(kBlobV2DescriptorFields.size()),
        schema.fields_size(),
        "Lance Blob v2 field '{}' is missing its storage descriptor fields",
        field.name());
    auto& children = result.childrenByParent[field.id()];
    children.reserve(kBlobV2DescriptorFields.size());
    for (const auto& [expectedName, expectedType] : kBlobV2DescriptorFields) {
      const auto& descriptor = schema.fields(++index);
      BOLT_CHECK_EQ(
          descriptor.id(),
          -1,
          "Lance Blob v2 descriptor field '{}' must have synthetic id -1",
          descriptor.name());
      BOLT_CHECK_EQ(
          descriptor.parent_id(),
          -1,
          "Lance Blob v2 descriptor field '{}' must have synthetic parent -1",
          descriptor.name());
      BOLT_CHECK_EQ(
          descriptor.name(),
          expectedName,
          "Unexpected Lance Blob v2 descriptor field order");
      BOLT_CHECK_EQ(
          descriptor.logical_type(),
          expectedType,
          "Unexpected Lance Blob v2 descriptor type for field '{}'",
          descriptor.name());
      children.push_back(&descriptor);
    }
  }
  return result;
}

bool isBlobColumnEncoding(
    const ::lance::file::v2::ColumnMetadata& column,
    uint32_t columnIndex) {
  // Early v2.0 fixtures did not always populate the column-level encoding.
  // The default interpretation is an ordinary values column.
  if (!column.has_encoding() ||
      column.encoding().location_case() ==
          ::lance::file::v2::Encoding::LOCATION_NOT_SET ||
      column.encoding().location_case() == ::lance::file::v2::Encoding::kNone) {
    return false;
  }
  BOLT_CHECK_EQ(
      column.encoding().location_case(),
      ::lance::file::v2::Encoding::kDirect,
      "Native Lance reader requires direct column encodings (column {})",
      columnIndex);
  google::protobuf::Any envelope;
  BOLT_CHECK(
      envelope.ParseFromString(column.encoding().direct().encoding()),
      "Invalid Lance column encoding envelope (column {})",
      columnIndex);
  BOLT_CHECK_EQ(
      envelope.type_url(),
      "/lance.encodings.ColumnEncoding",
      "Unsupported Lance column encoding type '{}' (column {})",
      envelope.type_url(),
      columnIndex);
  ::lance::encodings::ColumnEncoding encoding;
  BOLT_CHECK(
      encoding.ParseFromString(envelope.value()),
      "Invalid Lance column encoding (column {})",
      columnIndex);
  switch (encoding.column_encoding_case()) {
    case ::lance::encodings::ColumnEncoding::kValues:
      return false;
    case ::lance::encodings::ColumnEncoding::kBlob:
      BOLT_CHECK(
          encoding.blob().has_inner(),
          "Lance Blob column {} has no inner encoding",
          columnIndex);
      BOLT_CHECK_EQ(
          encoding.blob().inner().column_encoding_case(),
          ::lance::encodings::ColumnEncoding::kValues,
          "Native Lance reader requires a values inner encoding for Blob column {}",
          columnIndex);
      return true;
    case ::lance::encodings::ColumnEncoding::kZoneIndex:
      BOLT_UNSUPPORTED(
          "Native Lance reader does not support zone-index column {}",
          columnIndex);
    default:
      BOLT_UNSUPPORTED(
          "Unsupported Lance column encoding {} (column {})",
          static_cast<int>(encoding.column_encoding_case()),
          columnIndex);
  }
}

uint32_t mapPhysicalColumns(
    const ::lance::file::Field& field,
    const std::unordered_map<int32_t, std::vector<const ::lance::file::Field*>>&
        childrenByParent,
    const std::vector<::lance::file::v2::ColumnMetadata>& columns,
    uint32_t physicalColumnIndex,
    bool rowAligned,
    std::vector<std::string>& logicalTypes,
    std::vector<uint32_t>& spans,
    std::vector<std::vector<std::string>>& childLogicalTypes,
    std::vector<bool>& rowAlignedPhysicalColumns) {
  BOLT_CHECK_LT(physicalColumnIndex, columns.size());
  logicalTypes[physicalColumnIndex] = field.logical_type();
  rowAlignedPhysicalColumns[physicalColumnIndex] = rowAligned;
  if (field.logical_type() == "string" ||
      field.logical_type() == "large_string" ||
      field.logical_type() == "binary" ||
      field.logical_type() == "large_binary" ||
      field.logical_type() == "json") {
    const auto& column = columns[physicalColumnIndex];
    // Lance's early v2.0 writer used a List offsets column followed by a byte
    // column. Later v2.0 writers use Binary in a single physical column. An
    // empty field has no pages and needs no decoding, so one column is enough.
    const auto firstDataPage = std::find_if(
        column.pages().begin(), column.pages().end(), [](const auto& page) {
          return page.length() > 0;
        });
    if (firstDataPage == column.pages().end()) {
      return 1;
    }
    BOLT_CHECK(firstDataPage->has_encoding());
    BOLT_CHECK_EQ(
        firstDataPage->encoding().location_case(),
        ::lance::file::v2::Encoding::kDirect);
    google::protobuf::Any envelope;
    BOLT_CHECK(envelope.ParseFromString(
        firstDataPage->encoding().direct().encoding()));
    ::lance::encodings::ArrayEncoding encoding;
    BOLT_CHECK(encoding.ParseFromString(envelope.value()));
    const auto count = encoding.array_encoding_case() ==
            ::lance::encodings::ArrayEncoding::kList
        ? 2
        : 1;
    spans[physicalColumnIndex] = count;
    if (count == 2) {
      logicalTypes[physicalColumnIndex + 1] = "uint8";
      rowAlignedPhysicalColumns[physicalColumnIndex + 1] = false;
    }
    return count;
  }
  if (field.logical_type() == "struct") {
    const auto& column = columns[physicalColumnIndex];
    const auto firstDataPage = std::find_if(
        column.pages().begin(), column.pages().end(), [](const auto& page) {
          return page.length() > 0;
        });
    const auto legacyPacked = field.metadata().find("packed");
    const auto packedBySchema = legacyPacked != field.metadata().end()
        ? legacyPacked->second == "true"
        : field.metadata().contains("lance-encoding:packed");
    if (firstDataPage != column.pages().end()) {
      const auto& page = *firstDataPage;
      BOLT_CHECK(page.has_encoding());
      BOLT_CHECK_EQ(
          page.encoding().location_case(),
          ::lance::file::v2::Encoding::kDirect);
      google::protobuf::Any envelope;
      BOLT_CHECK(envelope.ParseFromString(page.encoding().direct().encoding()));
      ::lance::encodings::ArrayEncoding encoding;
      BOLT_CHECK(encoding.ParseFromString(envelope.value()));
      if (packedBySchema ||
          encoding.array_encoding_case() ==
              ::lance::encodings::ArrayEncoding::kPackedStruct) {
        if (auto it = childrenByParent.find(field.id());
            it != childrenByParent.end()) {
          for (const auto* child : it->second) {
            childLogicalTypes[physicalColumnIndex].push_back(
                child->logical_type());
          }
        }
        spans[physicalColumnIndex] = 1;
        return 1;
      }
    } else if (packedBySchema) {
      if (auto it = childrenByParent.find(field.id());
          it != childrenByParent.end()) {
        for (const auto* child : it->second) {
          childLogicalTypes[physicalColumnIndex].push_back(
              child->logical_type());
        }
      }
      spans[physicalColumnIndex] = 1;
      return 1;
    }
    uint32_t count = 1;
    if (auto it = childrenByParent.find(field.id());
        it != childrenByParent.end()) {
      for (const auto* child : it->second) {
        count += mapPhysicalColumns(
            *child,
            childrenByParent,
            columns,
            physicalColumnIndex + count,
            rowAligned,
            logicalTypes,
            spans,
            childLogicalTypes,
            rowAlignedPhysicalColumns);
      }
    }
    spans[physicalColumnIndex] = count;
    return count;
  }
  if (field.logical_type() == "list" || field.logical_type() == "large_list" ||
      field.logical_type() == "list.struct" ||
      field.logical_type() == "large_list.struct" ||
      field.logical_type() == "map") {
    uint32_t count = 1;
    if (auto it = childrenByParent.find(field.id());
        it != childrenByParent.end()) {
      for (const auto* child : it->second) {
        count += mapPhysicalColumns(
            *child,
            childrenByParent,
            columns,
            physicalColumnIndex + count,
            false,
            logicalTypes,
            spans,
            childLogicalTypes,
            rowAlignedPhysicalColumns);
      }
    }
    spans[physicalColumnIndex] = count;
    return count;
  }
  spans[physicalColumnIndex] = 1;
  return 1;
}

TypePtr convertField(
    const ::lance::file::Field& field,
    const std::unordered_map<int32_t, std::vector<const ::lance::file::Field*>>&
        childrenByParent,
    const std::shared_ptr<const NativeLanceTypeAdapter>& adapter) {
  const auto& logicalType = field.logical_type();
  auto childrenIt = childrenByParent.find(field.id());
  const auto hasChildren = childrenIt != childrenByParent.end();
  // Always validate extension-name agreement, including on complex fields.
  extensionName(field);

  if (isBlobV2Field(field)) {
    BOLT_CHECK(
        hasChildren && childrenIt->second.size() == 5,
        "Lance Blob v2 field '{}' must have five storage descriptor fields",
        field.name());
    return adaptSemanticType(field, VARBINARY(), adapter);
  }

  if (logicalType == "struct") {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    if (hasChildren) {
      names.reserve(childrenIt->second.size());
      types.reserve(childrenIt->second.size());
      for (const auto* child : childrenIt->second) {
        names.push_back(child->name());
        types.push_back(convertField(*child, childrenByParent, adapter));
      }
    }
    return adaptSemanticType(
        field, ROW(std::move(names), std::move(types)), adapter);
  }

  if (logicalType == "list" || logicalType == "large_list" ||
      logicalType == "list.struct" || logicalType == "large_list.struct") {
    BOLT_CHECK(
        hasChildren && childrenIt->second.size() == 1,
        "Lance list field '{}' must have exactly one child",
        field.name());
    return adaptSemanticType(
        field,
        ARRAY(convertField(
            *childrenIt->second.front(), childrenByParent, adapter)),
        adapter);
  }

  if (logicalType == "map") {
    BOLT_CHECK(
        hasChildren && childrenIt->second.size() == 1,
        "Lance map field '{}' must have one entries child",
        field.name());
    const auto* entries = childrenIt->second.front();
    auto entriesIt = childrenByParent.find(entries->id());
    BOLT_CHECK(
        entriesIt != childrenByParent.end() && entriesIt->second.size() == 2,
        "Lance map field '{}' must contain key and value children",
        field.name());
    return adaptSemanticType(
        field,
        MAP(convertField(*entriesIt->second[0], childrenByParent, adapter),
            convertField(*entriesIt->second[1], childrenByParent, adapter)),
        adapter);
  }

  if (logicalType.rfind("fixed_size_list:struct:", 0) == 0) {
    BOLT_CHECK(
        hasChildren && childrenIt->second.size() == 1,
        "Lance fixed-size-list field '{}' must have exactly one struct child",
        field.name());
    return adaptSemanticType(
        field,
        ARRAY(convertField(
            *childrenIt->second.front(), childrenByParent, adapter)),
        adapter);
  }

  BOLT_CHECK(
      !hasChildren,
      "Primitive Lance field '{}' unexpectedly has children",
      field.name());
  return adaptSemanticType(field, primitiveType(logicalType), adapter);
}

bool isPackedField(const ::lance::file::Field& field) {
  const auto legacyPacked = field.metadata().find("packed");
  return legacyPacked != field.metadata().end()
      ? legacyPacked->second == "true"
      : field.metadata().contains("lance-encoding:packed");
}

uint32_t mapStructuralPhysicalColumns(
    const ::lance::file::Field& field,
    const std::unordered_map<int32_t, std::vector<const ::lance::file::Field*>>&
        childrenByParent,
    uint32_t physicalColumnIndex,
    std::vector<std::string>& logicalTypes,
    std::vector<uint32_t>& spans,
    std::vector<std::vector<std::string>>& childLogicalTypes,
    std::vector<bool>& rowAlignedPhysicalColumns) {
  const auto children = childrenByParent.find(field.id());
  const auto isLeaf = children == childrenByParent.end() ||
      children->second.empty() || isPackedField(field) || isBlobField(field);
  if (isLeaf) {
    BOLT_CHECK_LT(physicalColumnIndex, logicalTypes.size());
    logicalTypes[physicalColumnIndex] = effectiveLogicalType(field);
    rowAlignedPhysicalColumns[physicalColumnIndex] = true;
    if (children != childrenByParent.end()) {
      for (const auto* child : children->second) {
        childLogicalTypes[physicalColumnIndex].push_back(child->logical_type());
      }
    }
    spans[physicalColumnIndex] = 1;
    return 1;
  }

  uint32_t count = 0;
  for (const auto* child : children->second) {
    count += mapStructuralPhysicalColumns(
        *child,
        childrenByParent,
        physicalColumnIndex + count,
        logicalTypes,
        spans,
        childLogicalTypes,
        rowAlignedPhysicalColumns);
  }
  BOLT_CHECK_GT(count, 0);
  spans[physicalColumnIndex] = count;
  return count;
}

NativeLanceMetadata::StructuralField makeStructuralField(
    const ::lance::file::Field& field,
    const std::unordered_map<int32_t, std::vector<const ::lance::file::Field*>>&
        childrenByParent,
    const std::shared_ptr<const NativeLanceTypeAdapter>& adapter,
    uint32_t& nextPhysicalColumn,
    uint64_t rowsPerParent = 1) {
  NativeLanceMetadata::StructuralField result;
  result.name = field.name();
  result.logicalType = effectiveLogicalType(field);
  result.type = convertField(field, childrenByParent, adapter);
  result.nullable = field.nullable();
  result.rowsPerParent = rowsPerParent;
  const auto children = childrenByParent.find(field.id());
  result.leaf = children == childrenByParent.end() ||
      children->second.empty() || isPackedField(field) || isBlobField(field);
  result.physicalColumnIndex = nextPhysicalColumn;
  if (result.leaf) {
    result.physicalColumnCount = 1;
    ++nextPhysicalColumn;
    return result;
  }
  auto childRowsPerParent = rowsPerParent;
  if (field.logical_type().rfind("fixed_size_list:struct:", 0) == 0) {
    const auto dimensionStart = field.logical_type().rfind(':');
    BOLT_CHECK_NE(dimensionStart, std::string::npos);
    const auto dimension =
        std::stoull(field.logical_type().substr(dimensionStart + 1));
    BOLT_CHECK_GT(dimension, 0);
    BOLT_CHECK_LE(
        childRowsPerParent, std::numeric_limits<uint64_t>::max() / dimension);
    childRowsPerParent *= dimension;
  }
  for (const auto* child : children->second) {
    result.children.push_back(makeStructuralField(
        *child,
        childrenByParent,
        adapter,
        nextPhysicalColumn,
        childRowsPerParent));
  }
  result.physicalColumnCount = nextPhysicalColumn - result.physicalColumnIndex;
  BOLT_CHECK_GT(result.physicalColumnCount, 0);
  return result;
}

RowTypePtr convertSchema(
    const SchemaTree& tree,
    const std::shared_ptr<const NativeLanceTypeAdapter>& adapter) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(tree.roots.size());
  types.reserve(tree.roots.size());
  for (const auto* root : tree.roots) {
    names.push_back(root->name());
    types.push_back(convertField(*root, tree.childrenByParent, adapter));
  }
  return ROW(std::move(names), std::move(types));
}

} // namespace

BufferPtr NativeLanceMetadata::read(uint64_t offset, uint64_t length) const {
  BOLT_CHECK_LE(offset, input_.getReadFile()->size());
  BOLT_CHECK_LE(length, input_.getReadFile()->size() - offset);
  BOLT_CHECK_LE(
      length,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "Lance metadata region is too large: {} bytes",
      length);
  auto buffer = AlignedBuffer::allocate<char>(length, &pool_);
  auto stream = input_.enqueue({offset, length});
  input_.load(dwio::common::LogType::HEADER);
  stream->readFully(buffer->asMutable<char>(), length);
  return buffer;
}

NativeLanceMetadata::NativeLanceMetadata(
    dwio::common::BufferedInput& input,
    memory::MemoryPool& pool,
    std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter)
    : input_(input), pool_(pool), typeAdapter_(std::move(typeAdapter)) {
  const auto fileSize = input_.getReadFile()->size();
  BOLT_CHECK_GE(fileSize, kFooterSize, "Lance file is too small: {}", fileSize);

  const auto footerBytes = read(fileSize - kFooterSize, kFooterSize);
  const auto* footer = footerBytes->as<char>();
  BOLT_CHECK_EQ(
      std::string_view(footer + 36, 4),
      "LANC",
      "Lance file has invalid footer magic");

  footer_ = Footer{
      .columnMetadataStart = readLittleEndian<uint64_t>(footer),
      .columnMetadataOffsetsStart = readLittleEndian<uint64_t>(footer + 8),
      .globalBufferOffsetsStart = readLittleEndian<uint64_t>(footer + 16),
      .numGlobalBuffers = readLittleEndian<uint32_t>(footer + 24),
      .numColumns = readLittleEndian<uint32_t>(footer + 28),
      .majorVersion = readLittleEndian<uint16_t>(footer + 32),
      .minorVersion = readLittleEndian<uint16_t>(footer + 34),
  };
  BOLT_CHECK(
      (footer_.majorVersion == 0 && footer_.minorVersion == 3) ||
          (footer_.majorVersion == 2 && footer_.minorVersion <= 3),
      "Native Lance reader currently supports v2.0-v2.3 files; got {}.{}",
      footer_.majorVersion,
      footer_.minorVersion);
  BOLT_CHECK_LE(
      footer_.columnMetadataStart, footer_.columnMetadataOffsetsStart);
  BOLT_CHECK_LE(
      footer_.columnMetadataOffsetsStart, footer_.globalBufferOffsetsStart);
  BOLT_CHECK_LE(footer_.globalBufferOffsetsStart, fileSize - kFooterSize);
  BOLT_CHECK_LE(
      footer_.numGlobalBuffers,
      (fileSize - kFooterSize - footer_.globalBufferOffsetsStart) / 16);
  BOLT_CHECK_EQ(
      footer_.globalBufferOffsetsStart +
          static_cast<uint64_t>(footer_.numGlobalBuffers) * 16,
      fileSize - kFooterSize,
      "Invalid Lance global buffer offset table");
  BOLT_CHECK_LE(
      footer_.numColumns,
      (footer_.globalBufferOffsetsStart - footer_.columnMetadataOffsetsStart) /
          16);
  BOLT_CHECK_EQ(
      footer_.columnMetadataOffsetsStart +
          static_cast<uint64_t>(footer_.numColumns) * 16,
      footer_.globalBufferOffsetsStart,
      "Invalid Lance column metadata offset table");

  const auto globalTable =
      read(footer_.globalBufferOffsetsStart, footer_.numGlobalBuffers * 16);
  globalBuffers_.reserve(footer_.numGlobalBuffers);
  for (uint32_t i = 0; i < footer_.numGlobalBuffers; ++i) {
    const auto* entry = globalTable->as<char>() + i * 16;
    BufferDescriptor descriptor{
        .offset = readLittleEndian<uint64_t>(entry),
        .length = readLittleEndian<uint64_t>(entry + 8),
    };
    BOLT_CHECK_LE(descriptor.offset, fileSize);
    BOLT_CHECK_LE(descriptor.length, fileSize - descriptor.offset);
    BOLT_CHECK_LE(
        descriptor.offset + descriptor.length,
        footer_.columnMetadataStart,
        "Lance global buffer {} overlaps column metadata",
        i);
    globalBuffers_.push_back(descriptor);
  }
  BOLT_CHECK(!globalBuffers_.empty(), "Lance file has no schema buffer");

  const auto schemaBytes =
      read(globalBuffers_[0].offset, globalBuffers_[0].length);
  ::lance::file::FileDescriptor descriptor;
  BOLT_CHECK(
      descriptor.ParseFromArray(
          schemaBytes->as<char>(), static_cast<int>(schemaBytes->size())),
      "Failed to parse Lance file descriptor");
  BOLT_CHECK(descriptor.has_schema(), "Lance file descriptor has no schema");
  numRows_ = descriptor.length();
  const auto schemaTree = buildSchemaTree(descriptor.schema());
  rowType_ = convertSchema(schemaTree, typeAdapter_);
  const auto& rootFields = schemaTree.roots;
  for (const auto* field : rootFields) {
    columnLogicalTypes_.emplace_back(effectiveLogicalType(*field));
  }
  BOLT_CHECK_EQ(columnLogicalTypes_.size(), rowType_->size());

  const auto columnTable =
      read(footer_.columnMetadataOffsetsStart, footer_.numColumns * 16);
  columnMetadataLocations_.reserve(footer_.numColumns);
  for (uint32_t i = 0; i < footer_.numColumns; ++i) {
    const auto* entry = columnTable->as<char>() + i * 16;
    const auto offset = readLittleEndian<uint64_t>(entry);
    const auto length = readLittleEndian<uint64_t>(entry + 8);
    BOLT_CHECK_GE(offset, footer_.columnMetadataStart);
    BOLT_CHECK_LE(offset, footer_.columnMetadataOffsetsStart);
    BOLT_CHECK_LE(length, footer_.columnMetadataOffsetsStart - offset);
    BOLT_CHECK_LE(
        length,
        static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
        "Lance column metadata {} is too large: {} bytes",
        i,
        length);
    columnMetadataLocations_.push_back({offset, length});
  }

  columns_.resize(footer_.numColumns);
  pageEncodings_.resize(footer_.numColumns);
  pageLayouts_.resize(footer_.numColumns);
  blobColumns_.resize(footer_.numColumns);
  columnMetadataLoaded_ = std::vector<std::atomic<bool>>(footer_.numColumns);
  physicalColumnExpectedRows_.resize(footer_.numColumns);
  if (!usesStructuralEncoding()) {
    std::vector<uint32_t> allColumns(footer_.numColumns);
    std::iota(allColumns.begin(), allColumns.end(), 0);
    loadPhysicalColumns(allColumns);
  }

  const auto& childrenByParent = schemaTree.childrenByParent;
  uint32_t nextPhysicalColumn = 0;
  if (usesStructuralEncoding()) {
    const auto appendLeafMapping =
        [&](const auto& self, const ::lance::file::Field& field) -> void {
      const auto children = childrenByParent.find(field.id());
      const auto isLeaf = children == childrenByParent.end() ||
          children->second.empty() || isPackedField(field) ||
          isBlobField(field);
      if (isLeaf) {
        BOLT_CHECK_LT(nextPhysicalColumn, columns_.size());
        leafPhysicalColumnIndices_.emplace(field.id(), nextPhysicalColumn++);
        return;
      }
      for (const auto* child : children->second) {
        self(self, *child);
      }
    };
    for (const auto* field : rootFields) {
      appendLeafMapping(appendLeafMapping, *field);
    }
    nextPhysicalColumn = 0;
    structuralFields_.reserve(rootFields.size());
    for (const auto* field : rootFields) {
      structuralFields_.push_back(makeStructuralField(
          *field, childrenByParent, typeAdapter_, nextPhysicalColumn));
    }
    BOLT_CHECK_EQ(nextPhysicalColumn, footer_.numColumns);
    nextPhysicalColumn = 0;
  }
  physicalColumnIndices_.reserve(rootFields.size());
  physicalColumnLogicalTypes_.resize(columns_.size());
  physicalColumnSpans_.resize(columns_.size(), 1);
  physicalColumnChildLogicalTypes_.resize(columns_.size());
  rowAlignedPhysicalColumns_.resize(columns_.size(), false);
  for (const auto* field : rootFields) {
    physicalColumnIndices_.push_back(nextPhysicalColumn);
    nextPhysicalColumn += usesStructuralEncoding()
        ? mapStructuralPhysicalColumns(
              *field,
              childrenByParent,
              nextPhysicalColumn,
              physicalColumnLogicalTypes_,
              physicalColumnSpans_,
              physicalColumnChildLogicalTypes_,
              rowAlignedPhysicalColumns_)
        : mapPhysicalColumns(
              *field,
              childrenByParent,
              columns_,
              nextPhysicalColumn,
              true,
              physicalColumnLogicalTypes_,
              physicalColumnSpans_,
              physicalColumnChildLogicalTypes_,
              rowAlignedPhysicalColumns_);
  }
  BOLT_CHECK_EQ(
      nextPhysicalColumn,
      footer_.numColumns,
      "Lance schema requires {} physical columns but footer declares {}",
      nextPhysicalColumn,
      footer_.numColumns);
  if (usesStructuralEncoding()) {
    const auto setExpectedRows = [&](const auto& self,
                                     const StructuralField& field) -> void {
      if (!field.leaf) {
        for (const auto& child : field.children) {
          self(self, child);
        }
        return;
      }
      BOLT_CHECK_LE(
          numRows_, std::numeric_limits<uint64_t>::max() / field.rowsPerParent);
      physicalColumnExpectedRows_[field.physicalColumnIndex] =
          numRows_ * field.rowsPerParent;
    };
    for (const auto& field : structuralFields_) {
      setExpectedRows(setExpectedRows, field);
    }
  } else {
    for (const auto columnIndex : physicalColumnIndices_) {
      uint64_t rows = 0;
      for (const auto& page : columns_[columnIndex].pages()) {
        BOLT_CHECK_LE(
            page.length(), std::numeric_limits<uint64_t>::max() - rows);
        rows += page.length();
      }
      BOLT_CHECK_EQ(
          rows,
          numRows_,
          "Lance physical column {} has {} rows but file descriptor declares {}",
          columnIndex,
          rows,
          numRows_);
    }
  }
}

void NativeLanceMetadata::parseColumnMetadata(
    uint32_t physicalColumnIndex,
    const char* data,
    size_t size) const {
  auto& column = columns_.at(physicalColumnIndex);
  BOLT_CHECK(
      column.ParseFromArray(data, static_cast<int>(size)),
      "Failed to parse Lance column metadata {}",
      physicalColumnIndex);
  blobColumns_[physicalColumnIndex] =
      isBlobColumnEncoding(column, physicalColumnIndex);
  auto& encodings = pageEncodings_[physicalColumnIndex];
  auto& layouts = pageLayouts_[physicalColumnIndex];
  encodings.reserve(column.pages_size());
  layouts.reserve(column.pages_size());
  const auto fileSize = input_.getReadFile()->size();
  for (int32_t pageIndex = 0; pageIndex < column.pages_size(); ++pageIndex) {
    const auto& page = column.pages(pageIndex);
    if (usesStructuralEncoding()) {
      layouts.push_back(parsePageLayout(page, physicalColumnIndex, pageIndex));
    } else {
      encodings.push_back(
          parsePageEncoding(page, physicalColumnIndex, pageIndex));
    }
    BOLT_CHECK_EQ(
        page.buffer_offsets_size(),
        page.buffer_sizes_size(),
        "Lance column {} page {} has mismatched buffer offsets and sizes",
        physicalColumnIndex,
        pageIndex);
    for (int32_t bufferIndex = 0; bufferIndex < page.buffer_offsets_size();
         ++bufferIndex) {
      const auto bufferOffset = page.buffer_offsets(bufferIndex);
      const auto bufferSize = page.buffer_sizes(bufferIndex);
      BOLT_CHECK_LE(bufferOffset, fileSize);
      BOLT_CHECK_LE(bufferSize, fileSize - bufferOffset);
      BOLT_CHECK_LE(
          bufferOffset + bufferSize,
          footer_.columnMetadataStart,
          "Lance column {} page {} buffer {} overlaps column metadata",
          physicalColumnIndex,
          pageIndex,
          bufferIndex);
    }
  }
  BOLT_CHECK_EQ(
      column.buffer_offsets_size(),
      column.buffer_sizes_size(),
      "Lance column {} has mismatched column buffer offsets and sizes",
      physicalColumnIndex);
  for (int32_t bufferIndex = 0; bufferIndex < column.buffer_offsets_size();
       ++bufferIndex) {
    const auto bufferOffset = column.buffer_offsets(bufferIndex);
    const auto bufferSize = column.buffer_sizes(bufferIndex);
    BOLT_CHECK_LE(bufferOffset, fileSize);
    BOLT_CHECK_LE(bufferSize, fileSize - bufferOffset);
    BOLT_CHECK_LE(
        bufferOffset + bufferSize,
        footer_.columnMetadataStart,
        "Lance column {} buffer {} overlaps column metadata",
        physicalColumnIndex,
        bufferIndex);
  }
  validateColumnMetadata(physicalColumnIndex);
}

void NativeLanceMetadata::validateColumnMetadata(
    uint32_t physicalColumnIndex) const {
  const auto expectedRows = physicalColumnExpectedRows_[physicalColumnIndex];
  if (expectedRows == 0 && numRows_ != 0) {
    return;
  }
  uint64_t rows = 0;
  for (const auto& page : columns_[physicalColumnIndex].pages()) {
    BOLT_CHECK_LE(page.length(), std::numeric_limits<uint64_t>::max() - rows);
    rows += page.length();
  }
  BOLT_CHECK_EQ(
      rows,
      expectedRows,
      "Lance physical column {} has {} rows but schema requires {}",
      physicalColumnIndex,
      rows,
      expectedRows);
}

void NativeLanceMetadata::loadPhysicalColumns(
    const std::vector<uint32_t>& physicalColumnIndices) const {
  const auto allLoaded = std::all_of(
      physicalColumnIndices.begin(),
      physicalColumnIndices.end(),
      [&](const auto index) {
        BOLT_CHECK_LT(index, footer_.numColumns);
        return columnMetadataLoaded_[index].load(std::memory_order_acquire);
      });
  if (allLoaded) {
    return;
  }
  std::lock_guard<std::mutex> guard(columnMetadataMutex_);
  struct Read {
    uint32_t index;
    BufferDescriptor descriptor;
    std::unique_ptr<dwio::common::SeekableInputStream> stream;
  };
  std::vector<Read, memory::StlAllocator<Read>> reads{
      memory::StlAllocator<Read>(&pool_)};
  for (const auto index : physicalColumnIndices) {
    BOLT_CHECK_LT(index, footer_.numColumns);
    if (columnMetadataLoaded_[index].load(std::memory_order_relaxed)) {
      continue;
    }
    const auto descriptor = columnMetadataLocations_[index];
    reads.push_back(
        {index,
         descriptor,
         input_.enqueue({descriptor.offset, descriptor.length})});
  }
  if (!reads.empty()) {
    input_.load(dwio::common::LogType::HEADER);
  }
  try {
    for (auto& read : reads) {
      auto bytes =
          AlignedBuffer::allocate<char>(read.descriptor.length, &pool_);
      read.stream->readFully(bytes->asMutable<char>(), read.descriptor.length);
      parseColumnMetadata(read.index, bytes->as<char>(), bytes->size());
    }
    for (const auto& read : reads) {
      columnMetadataLoaded_[read.index].store(true, std::memory_order_release);
    }
  } catch (...) {
    for (const auto& read : reads) {
      columns_[read.index].Clear();
      pageEncodings_[read.index].clear();
      pageLayouts_[read.index].clear();
      blobColumns_[read.index] = false;
      columnMetadataLoaded_[read.index].store(false, std::memory_order_relaxed);
    }
    throw;
  }
}

void NativeLanceMetadata::loadLogicalColumns(
    const std::vector<uint32_t>& columnIndices) const {
  std::vector<uint32_t> physicalColumns;
  for (const auto columnIndex : columnIndices) {
    BOLT_CHECK_LT(columnIndex, physicalColumnIndices_.size());
    const auto first = physicalColumnIndices_[columnIndex];
    const auto count = usesStructuralEncoding()
        ? structuralFields_[columnIndex].physicalColumnCount
        : physicalColumnSpans_[first];
    for (uint32_t offset = 0; offset < count; ++offset) {
      physicalColumns.push_back(first + offset);
    }
  }
  std::sort(physicalColumns.begin(), physicalColumns.end());
  physicalColumns.erase(
      std::unique(physicalColumns.begin(), physicalColumns.end()),
      physicalColumns.end());
  loadPhysicalColumns(physicalColumns);
}

const ::lance::file::v2::ColumnMetadata& NativeLanceMetadata::column(
    uint32_t physicalColumnIndex) const {
  loadPhysicalColumns({physicalColumnIndex});
  return columns_.at(physicalColumnIndex);
}

const std::vector<::lance::file::v2::ColumnMetadata>&
NativeLanceMetadata::columns() const {
  std::vector<uint32_t> all(footer_.numColumns);
  std::iota(all.begin(), all.end(), 0);
  loadPhysicalColumns(all);
  return columns_;
}

const ::lance::encodings::ArrayEncoding& NativeLanceMetadata::pageEncoding(
    uint32_t physicalColumnIndex,
    int32_t pageIndex) const {
  loadPhysicalColumns({physicalColumnIndex});
  return pageEncodings_.at(physicalColumnIndex).at(pageIndex);
}

const ::lance::encodings21::PageLayout& NativeLanceMetadata::pageLayout(
    uint32_t physicalColumnIndex,
    int32_t pageIndex) const {
  loadPhysicalColumns({physicalColumnIndex});
  return pageLayouts_.at(physicalColumnIndex).at(pageIndex);
}

size_t NativeLanceMetadata::loadedColumnMetadataCount() const {
  return std::count_if(
      columnMetadataLoaded_.begin(),
      columnMetadataLoaded_.end(),
      [](const auto& loaded) {
        return loaded.load(std::memory_order_acquire);
      });
}

std::vector<std::pair<uint64_t, uint64_t>>
NativeLanceMetadata::rowRangesForFileRange(uint64_t offset, uint64_t limit)
    const {
  BOLT_CHECK_LE(offset, limit);
  if (numRows_ == 0) {
    return {};
  }

  const ::lance::file::v2::ColumnMetadata* anchor = nullptr;
  for (uint32_t physicalIndex = 0; physicalIndex < columns_.size();
       ++physicalIndex) {
    if (!rowAlignedPhysicalColumns_[physicalIndex]) {
      continue;
    }
    loadPhysicalColumns({physicalIndex});
    const auto& candidate = columns_[physicalIndex];
    const auto allPagesHaveBuffers = std::all_of(
        candidate.pages().begin(),
        candidate.pages().end(),
        [](const auto& page) {
          return page.length() == 0 || page.buffer_offsets_size() > 0;
        });
    const auto candidateRows = std::accumulate(
        candidate.pages().begin(),
        candidate.pages().end(),
        uint64_t{0},
        [](uint64_t rows, const auto& page) { return rows + page.length(); });
    if (allPagesHaveBuffers && candidateRows == numRows_) {
      anchor = &candidate;
      break;
    }
  }

  // All-null and structural-only files may not have any data buffers. Treat
  // them as a single page rooted at byte zero so exactly one split owns them.
  if (anchor == nullptr) {
    return offset == 0
        ? std::vector<std::pair<uint64_t, uint64_t>>{{0, numRows_}}
        : std::vector<std::pair<uint64_t, uint64_t>>{};
  }

  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  uint64_t rowStart = 0;
  for (const auto& page : anchor->pages()) {
    const auto rowEnd = rowStart + page.length();
    if (page.length() == 0) {
      continue;
    }
    const auto pageOffset = *std::min_element(
        page.buffer_offsets().begin(), page.buffer_offsets().end());
    if (pageOffset >= offset && pageOffset < limit && rowStart < rowEnd) {
      if (!ranges.empty() && ranges.back().second == rowStart) {
        ranges.back().second = rowEnd;
      } else {
        ranges.emplace_back(rowStart, rowEnd);
      }
    }
    rowStart = rowEnd;
  }
  BOLT_CHECK_EQ(rowStart, numRows_);
  return ranges;
}

} // namespace bytedance::bolt::lance::reader

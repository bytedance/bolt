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

/*
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/substrait/BoltExtensions.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/HiveDataSink.h"
#include "bolt/connectors/hive/PaimonConnectorSplit.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Split.h"
#include "bolt/exec/TableWriter.h"
#include "bolt/substrait/BoltToSubstraitType.h"
#include "bolt/substrait/proto/substrait/bolt/extensions.pb.h"
#include "bolt/tpch/gen/TpchGen.h"

namespace bytedance::bolt::substrait {

void setBoltFileFormat(
    ::substrait::bolt::FileFormat* ff,
    bolt::dwio::common::FileFormat fileFormat) {
  switch (fileFormat) {
    case dwio::common::FileFormat::ORC:
      ff->mutable_orc();
      break;
    case dwio::common::FileFormat::PARQUET:
      ff->mutable_parquet();
      break;
    case dwio::common::FileFormat::DWRF:
      ff->mutable_dwrf();
      break;
    case dwio::common::FileFormat::TEXT:
      ff->mutable_text();
      break;
    default:
      break;
  }
}

bolt::dwio::common::FileFormat toDwioFileFormat(
    const ::substrait::bolt::FileFormat& ff) {
  if (ff.has_orc()) {
    return dwio::common::FileFormat::ORC;
  }
  if (ff.has_parquet()) {
    return dwio::common::FileFormat::PARQUET;
  }
  if (ff.has_dwrf()) {
    return dwio::common::FileFormat::DWRF;
  }
  if (ff.has_text()) {
    return dwio::common::FileFormat::TEXT;
  }
  return dwio::common::FileFormat::UNKNOWN;
}

void fillSubstraitFileOrFiles(
    const ::substrait::bolt::FileFormat& ff,
    ::substrait::ReadRel::LocalFiles::FileOrFiles* out) {
  if (!out) {
    return;
  }
  if (ff.has_orc()) {
    out->mutable_orc();
  } else if (ff.has_parquet()) {
    out->mutable_parquet();
  } else if (ff.has_dwrf()) {
    out->mutable_dwrf();
  } else if (ff.has_text()) {
    out->mutable_text();
  }
}

::substrait::bolt::HiveExtension makeHiveExtension(
    bolt::dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& partitionKeys,
    int32_t numBuckets,
    const std::vector<std::string>& bucketKeys) {
  ::substrait::bolt::HiveExtension ext;
  setBoltFileFormat(ext.mutable_file_format(), fileFormat);
  for (const auto& k : partitionKeys) {
    ext.add_partition_keys(k);
  }
  if (numBuckets > 0) {
    ext.set_bucket_count(static_cast<uint64_t>(numBuckets));
  }
  for (const auto& k : bucketKeys) {
    ext.add_bucket_keys(k);
  }
  return ext;
}

::substrait::ReadRel makePaimonExtensionTable(
    const RowTypePtr& schema,
    bolt::dwio::common::FileFormat fileFormat,
    const std::vector<bolt::exec::Split>& splits,
    const std::unordered_map<std::string, std::string>& parameters) {
  ::substrait::ReadRel readRel;

  // Convert schema to NamedStruct and set base_schema.
  google::protobuf::Arena arena;
  BoltToSubstraitTypeConvertor typeConv;
  const auto& named = typeConv.toSubstraitNamedStruct(arena, schema);
  readRel.mutable_base_schema()->MergeFrom(named);

  // Build extension table detail using PaimonExtensionTable.
  ::substrait::bolt::PaimonExtensionTable table;

  // Parameters as Expression.Literal.Map (optional).
  if (!parameters.empty()) {
    auto* params = table.mutable_parameters();
    for (const auto& kv : parameters) {
      auto* entry = params->add_key_values();
      entry->mutable_key()->set_string(kv.first);
      entry->mutable_value()->set_string(kv.second);
    }
  }

  // File format mapping (extensions.proto now nests formats under file_format).
  setBoltFileFormat(table.mutable_file_format(), fileFormat);

  // Splits to PaimonSplit(file_uris) with full file paths.
  for (const auto& split : splits) {
    auto* s = table.add_splits();
    if (!split.connectorSplit) {
      continue;
    }

    if (auto* ps = dynamic_cast<bolt::connector::hive::PaimonConnectorSplit*>(
            split.connectorSplit.get())) {
      for (const auto& hs : ps->hiveSplits) {
        if (hs) {
          s->add_file_uris(hs->filePath);
        }
      }
    } else if (
        auto* hs = dynamic_cast<bolt::connector::hive::HiveConnectorSplit*>(
            split.connectorSplit.get())) {
      s->add_file_uris(hs->filePath);
    }
  }

  // Pack into ReadRel.extension_table.detail.
  readRel.mutable_extension_table()->mutable_detail()->PackFrom(table);
  return readRel;
}

::substrait::ReadRel makeTpchExtensionTable(
    const std::string& tableName,
    const std::vector<std::string>& columnNames,
    double scaleFactor,
    uint64_t numSplits,
    const std::string& connectorId) {
  ::substrait::ReadRel readRel;

  // Derive schema from TPCH definitions for provided columns (no prefix).
  auto tableEnum = tpch::fromTableName(tableName);
  auto fullSchema =
      tpch::getTableSchema(tableEnum, /*usePrefixInColumnName*/ false);
  std::vector<TypePtr> types;
  types.reserve(columnNames.size());
  for (const auto& name : columnNames) {
    auto childType = fullSchema->findChild(name);
    BOLT_CHECK_NOT_NULL(
        childType, "Column '{}' not found in TPCH table '{}'", name, tableName);
    types.emplace_back(childType);
  }
  // ROW expects rvalue vectors; copy names into a local vector and move.
  std::vector<std::string> names = columnNames;
  auto rowType = ROW(std::move(names), std::move(types));

  google::protobuf::Arena arena;
  BoltToSubstraitTypeConvertor typeConv;
  const auto& named = typeConv.toSubstraitNamedStruct(arena, rowType);
  readRel.mutable_base_schema()->MergeFrom(named);

  // Fill extension table detail.
  ::substrait::bolt::TpchExtensionTable ext;
  ext.set_table_name(tableName);
  for (const auto& c : columnNames) {
    ext.add_column_names(c);
  }
  // Two scale-factor fields for proto wire compat. Old readers
  // (pre-double migration) read ``scale_factor_deprecated`` as
  // uint64; we populate it with a best-effort integer cast so
  // whole-number scales still work for them. New readers read the
  // precise ``scale_factor_double`` and ignore the deprecated
  // field. See ``TpchExtensionTable`` doc-comment for the
  // wire-compat rationale.
  ext.set_scale_factor_deprecated(static_cast<uint64_t>(scaleFactor));
  ext.set_scale_factor_double(scaleFactor);
  ext.set_num_splits(numSplits);
  ext.set_connector_id(connectorId);

  readRel.mutable_extension_table()->mutable_detail()->PackFrom(ext);
  return readRel;
}

::substrait::ExtensionSingleRel createShuffleRel(
    const ::substrait::Rel& input,
    int64_t seed) {
  ::substrait::ExtensionSingleRel rel;
  *rel.mutable_input() = input;

  // Pack LocalShuffleExtension with the given seed into detail.
  ::substrait::bolt::LocalShuffleExtension ext;
  ext.set_seed(seed);
  rel.mutable_detail()->PackFrom(ext);

  // Mark as direct to avoid emit errors.
  rel.mutable_common()->mutable_direct();
  return rel;
}

bolt::core::PlanNodePtr makeShufflePlanNode(
    const std::string& id,
    const ::substrait::ExtensionSingleRel& rel,
    const bolt::core::PlanNodePtr& input) {
  if (!rel.detail().Is<::substrait::bolt::LocalShuffleExtension>()) {
    return nullptr;
  }
  ::substrait::bolt::LocalShuffleExtension ext;
  rel.detail().UnpackTo(&ext);
  int64_t seed = ext.seed();

  return std::make_shared<core::LocalShuffleNode>(id, seed, input);
}

::substrait::WriteRel createWriteRel(
    const std::string& directoryPath,
    bolt::dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& partitionKeys,
    int32_t numBuckets,
    const std::vector<std::string>& bucketKeys) {
  ::substrait::WriteRel write;

  // Target table encoded as a single path component.
  auto* named = write.mutable_named_table();
  named->add_names(directoryPath);

  // Default schema and modes; consumer must ensure schema alignment.
  write.set_op(::substrait::WriteRel::WRITE_OP_INSERT);
  write.set_create_mode(::substrait::WriteRel::CREATE_MODE_REPLACE_IF_EXISTS);
  write.set_output(::substrait::WriteRel::OUTPUT_MODE_NO_OUTPUT);

  // Advanced extension payload using HiveExtension (protobuf) for write
  // options.
  auto ext =
      makeHiveExtension(fileFormat, partitionKeys, numBuckets, bucketKeys);
  write.mutable_advanced_extension()->mutable_enhancement()->PackFrom(ext);

  return write;
}

bolt::core::PlanNodePtr makeWritePlanNode(
    const ::substrait::WriteRel& writeRel,
    const bolt::core::PlanNodePtr& input,
    const std::string& planNodeId) {
  using bytedance::bolt::connector::hive::HiveBucketProperty;
  using bytedance::bolt::connector::hive::HiveColumnHandle;
  using bytedance::bolt::connector::hive::HiveInsertTableHandle;
  using bytedance::bolt::connector::hive::HiveSortingColumn;
  using bytedance::bolt::connector::hive::LocationHandle;

  auto rowType = input->outputType();

  // Parse advanced options.
  std::vector<std::string> partitionBy;
  int32_t bucketCount{0};
  std::vector<std::string> bucketedBy;
  dwio::common::FileFormat fileFormat = dwio::common::FileFormat::UNKNOWN;

  if (writeRel.has_advanced_extension()) {
    const auto& adv = writeRel.advanced_extension();
    ::substrait::bolt::HiveExtension ext;
    bool unpacked = false;
    if (adv.has_enhancement()) {
      unpacked = adv.enhancement().UnpackTo(&ext);
    }
    if (!unpacked && adv.optimization_size() > 0) {
      unpacked = adv.optimization(0).UnpackTo(&ext);
    }
    if (unpacked) {
      partitionBy.assign(
          ext.partition_keys().begin(), ext.partition_keys().end());
      bucketCount = static_cast<int32_t>(ext.bucket_count());
      bucketedBy.assign(ext.bucket_keys().begin(), ext.bucket_keys().end());
      if (ext.has_file_format()) {
        const auto& ff = ext.file_format();
        auto fmt = toDwioFileFormat(ff);
        if (fileFormat == dwio::common::FileFormat::UNKNOWN) {
          fileFormat = fmt;
        } else if (fileFormat != fmt) {
          BOLT_FAIL(
              "Bolt substrait write relation does not support multiple file formats.");
        }
      }
    }
  }

  if (fileFormat == dwio::common::FileFormat::UNKNOWN) {
    BOLT_FAIL("No supported file format specified in substrait write rel.");
  }

  // Build Hive column handles and tag partition columns.
  std::vector<std::shared_ptr<const HiveColumnHandle>> columns;
  columns.reserve(rowType->size());
  for (auto i = 0; i < rowType->size(); ++i) {
    const auto& name = rowType->nameOf(i);
    const bool isPartitionKey =
        std::find(partitionBy.begin(), partitionBy.end(), name) !=
        partitionBy.end();
    columns.push_back(std::make_shared<HiveColumnHandle>(
        name,
        isPartitionKey ? HiveColumnHandle::ColumnType::kPartitionKey
                       : HiveColumnHandle::ColumnType::kRegular,
        rowType->childAt(i),
        rowType->childAt(i)));
  }

  // Target directory path from named_table.
  std::string targetPath;
  if (writeRel.has_named_table()) {
    const auto& named = writeRel.named_table();
    for (int i = 0; i < named.names_size(); ++i) {
      if (i > 0) {
        targetPath += "/";
      }
      targetPath += named.names(i);
    }
  }

  auto location = std::make_shared<LocationHandle>(
      targetPath, targetPath, LocationHandle::TableType::kNew, "");

  std::shared_ptr<HiveBucketProperty> bucketProperty;
  if (bucketCount > 0 && !bucketedBy.empty()) {
    std::vector<TypePtr> bucketTypes;
    bucketTypes.reserve(bucketedBy.size());
    for (const auto& k : bucketedBy) {
      bucketTypes.push_back(rowType->childAt(rowType->getChildIdx(k)));
    }
    std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy;
    bucketProperty = std::make_shared<HiveBucketProperty>(
        HiveBucketProperty::Kind::kHiveCompatible,
        bucketCount,
        bucketedBy,
        bucketTypes,
        sortedBy);
  }

  auto hiveInsert = std::make_shared<HiveInsertTableHandle>(
      columns, location, fileFormat, bucketProperty);

  auto insertHandle = std::make_shared<core::InsertTableHandle>(
      std::string("test-hive"), hiveInsert);

  std::shared_ptr<core::AggregationNode> statsAgg = nullptr;

  return std::make_shared<core::TableWriteNode>(
      planNodeId,
      rowType,
      rowType->names(),
      statsAgg,
      insertHandle,
      /*outputBatchDWRF*/ false,
      exec::TableWriteTraits::outputType(statsAgg),
      connector::CommitStrategy::kNoCommit,
      input);
}

// Create a SimpleExtensionDeclaration for canonical "UNKNOWN" type.
::substrait::extensions::SimpleExtensionDeclaration makeUnknownTypeExtension(
    uint32_t index) {
  ::substrait::extensions::SimpleExtensionDeclaration decl;
  auto* typeDecl = decl.mutable_extension_type();
  typeDecl->set_extension_uri_reference(index);
  typeDecl->set_type_anchor(index);
  typeDecl->set_name("UNKNOWN");
  return decl;
}

bool isSupportedTypeExtensionName(const std::string& name) {
  return name == "UNKNOWN";
}
} // namespace bytedance::bolt::substrait

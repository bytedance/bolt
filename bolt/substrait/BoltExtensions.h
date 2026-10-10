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

#pragma once

#include <unordered_map>
#include <vector>

#include "bolt/core/PlanNode.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Split.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/substrait/proto/substrait/bolt/extensions.pb.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::substrait {

// Map dwio::common::FileFormat -> Substrait bolt FileFormat message.
void setBoltFileFormat(
    ::substrait::bolt::FileFormat* ff,
    bolt::dwio::common::FileFormat fileFormat);

// Map Substrait bolt FileFormat -> dwio::common::FileFormat.
bolt::dwio::common::FileFormat toDwioFileFormat(
    const ::substrait::bolt::FileFormat& ff);

// Fill a Substrait ReadRel.LocalFiles.FileOrFiles stub from a bolt FileFormat.
void fillSubstraitFileOrFiles(
    const ::substrait::bolt::FileFormat& ff,
    ::substrait::ReadRel::LocalFiles::FileOrFiles* out);

// Build Hive write advanced extension message from options.
::substrait::bolt::HiveExtension makeHiveExtension(
    bolt::dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& partitionKeys,
    int32_t numBuckets,
    const std::vector<std::string>& bucketKeys);

// Build a Substrait ReadRel for Paimon extension table.
::substrait::ReadRel makePaimonExtensionTable(
    const bolt::RowTypePtr& schema,
    bolt::dwio::common::FileFormat fileFormat,
    const std::vector<bolt::exec::Split>& splits,
    const std::unordered_map<std::string, std::string>& parameters);

// Build a Substrait ReadRel for TPCH generator using TpchExtensionTable.
::substrait::ReadRel makeTpchExtensionTable(
    const std::string& tableName,
    const std::vector<std::string>& columnNames,
    double scaleFactor,
    uint64_t numSplits,
    const std::string& connectorId);

// Build a Substrait ExtensionSingleRel that encodes a local shuffle using
// LocalShuffleExtension with a seed argument.
::substrait::ExtensionSingleRel createShuffleRel(
    const ::substrait::Rel& input,
    int64_t seed);

// Convert the shuffle rel into a Bolt LocalShuffleNode with the provided
// input. ``id`` is the PlanNode id assigned by the caller; the converter
// owns id allocation so multiple shuffles in the same plan get distinct
// ids.
bolt::core::PlanNodePtr makeShufflePlanNode(
    const std::string& id,
    const ::substrait::ExtensionSingleRel& rel,
    const bolt::core::PlanNodePtr& input);

// Build a Substrait WriteRel that targets a directory path and carries
// partitioning/bucketing options in advanced_extension as HiveExtension.
::substrait::WriteRel createWriteRel(
    const std::string& directoryPath,
    bolt::dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& partitionKeys,
    int32_t numBuckets,
    const std::vector<std::string>& bucketKeys);

// Build a TableWrite plan node from a WriteRel and an upstream input node.
bolt::core::PlanNodePtr makeWritePlanNode(
    const ::substrait::WriteRel& writeRel,
    const bolt::core::PlanNodePtr& input,
    const std::string& planNodeId);

// Returns a SimpleExtensionDeclaration for the canonical 'UNKNOWN' type.
::substrait::extensions::SimpleExtensionDeclaration makeUnknownTypeExtension(
    uint32_t index);

// Returns true if the provided type extension name is supported.
bool isSupportedTypeExtensionName(const std::string& name);
} // namespace bytedance::bolt::substrait

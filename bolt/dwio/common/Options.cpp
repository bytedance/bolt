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
 *
 * --------------------------------------------------------------------------
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

#include "bolt/dwio/common/Options.h"
#include <sstream>
#include "bolt/type/Type.h"
namespace bytedance::bolt::dwio::common {

FileFormat toFileFormat(std::string s) {
  if (s == "dwrf") {
    return FileFormat::DWRF;
  } else if (s == "rc") {
    return FileFormat::RC;
  } else if (s == "rc:text") {
    return FileFormat::RC_TEXT;
  } else if (s == "rc:binary") {
    return FileFormat::RC_BINARY;
  } else if (s == "text") {
    return FileFormat::TEXT;
  } else if (s == "json") {
    return FileFormat::JSON;
  } else if (s == "parquet") {
    return FileFormat::PARQUET;
  } else if (s == "alpha") {
    return FileFormat::ALPHA;
  } else if (s == "orc") {
    return FileFormat::ORC;
  }
  return FileFormat::UNKNOWN;
}

std::string toString(FileFormat fmt) {
  switch (fmt) {
    case FileFormat::DWRF:
      return "dwrf";
    case FileFormat::RC:
      return "rc";
    case FileFormat::RC_TEXT:
      return "rc:text";
    case FileFormat::RC_BINARY:
      return "rc:binary";
    case FileFormat::TEXT:
      return "text";
    case FileFormat::JSON:
      return "json";
    case FileFormat::PARQUET:
      return "parquet";
    case FileFormat::ALPHA:
      return "alpha";
    case FileFormat::ORC:
      return "orc";
    default:
      return "unknown";
  }
}

std::string ReaderOptions::toString() const {
  std::stringstream ss;
  ss << "ReaderOptions: " << std::endl;
  ss << "  loadQuantum_: " << loadQuantum_ << std::endl;
  ss << "  maxCoalesceDistance_: " << maxCoalesceDistance_ << std::endl;
  ss << "  maxCoalesceBytes_: " << maxCoalesceBytes_ << std::endl;
  ss << "  prefetchRowGroups_: " << prefetchRowGroups_ << std::endl;
  ss << "  fileFormat: " << fileFormat << std::endl;
  return ss.str();
}

ColumnReaderOptions makeColumnReaderOptions(const ReaderOptions& options) {
  ColumnReaderOptions columnReaderOptions;
  columnReaderOptions.useColumnNamesForColumnMapping_ =
      options.isUseColumnNamesForColumnMapping();
  return columnReaderOptions;
}

// We do *not* serialize pool, nonReclaimableSection, or the callbacks /
//  executor inside spillConfig—they must be re‐injected by the host.
folly::dynamic WriterOptions::serialize() const {
  folly::dynamic obj = folly::dynamic::object;

  if (schema) {
    obj["schema"] = schema->serialize();
  }

  if (compressionKind) {
    obj["compressionKind"] = static_cast<int>(*compressionKind);
  }

  if (!serdeParameters.empty()) {
    folly::dynamic mapObj = folly::dynamic::object;
    for (const auto& [k, v] : serdeParameters) {
      mapObj[k] = v;
    }
    obj["serdeParameters"] = std::move(mapObj);
  }

  if (maxStripeSize) {
    obj["maxStripeSize"] = static_cast<int64_t>(*maxStripeSize);
  }

  if (arrowBridgeTimestampUnit) {
    obj["arrowBridgeTimestampUnit"] =
        static_cast<int64_t>(*arrowBridgeTimestampUnit);
  }

  if (zlibCompressionLevel) {
    obj["zlibCompressionLevel"] = static_cast<int64_t>(*zlibCompressionLevel);
  }

  // spillConfig (value fields only; callbacks/executor re-injected by host)
  if (spillConfig) {
    obj["spillConfig"] = spillConfig->serialize();
  }

  return obj;
}

// pool, nonReclaimableSection, and spillConfig callbacks/executor remain at
// default and must be re-injected by the host.
std::shared_ptr<WriterOptions> WriterOptions::deserialize(
    const folly::dynamic& obj) {
  auto opts = std::make_shared<WriterOptions>();

  // 1) schema
  if (auto p = obj.get_ptr("schema")) {
    opts->schema = ISerializable::deserialize<bolt::Type>(*p);
  }

  // 2) compressionKind
  if (auto p = obj.get_ptr("compressionKind")) {
    opts->compressionKind =
        static_cast<bolt::common::CompressionKind>(p->asInt());
  }

  // 3) serdeParameters
  if (auto p = obj.get_ptr("serdeParameters")) {
    opts->serdeParameters.clear();
    for (auto& kv : p->items()) {
      opts->serdeParameters.emplace(kv.first.asString(), kv.second.asString());
    }
  }

  // 4) maxStripeSize
  if (auto p = obj.get_ptr("maxStripeSize")) {
    opts->maxStripeSize = static_cast<uint64_t>(p->asInt());
  }

  // 5) arrowBridgeTimestampUnit
  if (auto p = obj.get_ptr("arrowBridgeTimestampUnit")) {
    opts->arrowBridgeTimestampUnit = static_cast<uint8_t>(p->asInt());
  }

  // 6) zlibCompressionLevel
  if (auto p = obj.get_ptr("zlibCompressionLevel")) {
    opts->zlibCompressionLevel = static_cast<uint8_t>(p->asInt());
  }

  // 7) spillConfig
  if (auto p = obj.get_ptr("spillConfig")) {
    opts->ownedSpillConfig =
        ISerializable::deserialize<bolt::common::SpillConfig>(*p);
    opts->spillConfig = opts->ownedSpillConfig.get();
  }

  return opts;
}

void WriterOptions::registerSerDe() {
  bolt::Type::registerSerDe();
  bolt::common::SpillConfig::registerSerDe();
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("WriterOptions", WriterOptions::deserialize);
}

} // namespace bytedance::bolt::dwio::common

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

folly::dynamic SerDeOptions::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  folly::dynamic sepArray = folly::dynamic::array;
  for (uint8_t sep : separators) {
    sepArray.push_back(static_cast<int64_t>(sep));
  }
  obj["separators"] = sepArray;
  obj["nullString"] = nullString;
  obj["lastColumnTakesRest"] = lastColumnTakesRest;
  obj["escapeChar"] = static_cast<int64_t>(escapeChar);
  obj["isEscaped"] = isEscaped;
  return obj;
}

SerDeOptions SerDeOptions::deserialize(const folly::dynamic& obj) {
  SerDeOptions options;
  if (const auto* p = obj.get_ptr("separators")) {
    if (p->isArray()) {
      auto sepArray = *p;
      for (size_t i = 0;
           i < std::min(sepArray.size(), options.separators.size());
           ++i) {
        options.separators[i] = static_cast<uint8_t>(sepArray[i].asInt());
      }
    }
  }
  if (const auto* p = obj.get_ptr("nullString")) {
    if (p->isString()) {
      options.nullString = p->asString();
    }
  }
  if (const auto* p = obj.get_ptr("lastColumnTakesRest")) {
    if (p->isBool()) {
      options.lastColumnTakesRest = p->asBool();
    }
  }
  if (const auto* p = obj.get_ptr("escapeChar")) {
    if (p->isNumber()) {
      options.escapeChar = static_cast<uint8_t>(p->asInt());
    }
  }
  if (const auto* p = obj.get_ptr("isEscaped")) {
    if (p->isBool()) {
      options.isEscaped = p->asBool();
    }
  }
  return options;
}

SerDeOptions SerDeOptions::create(const folly::dynamic& obj) {
  return deserialize(obj);
}

folly::dynamic ReaderOptions::serialize() const {
  folly::dynamic obj = io::ReaderOptions::serialize();
  obj["tailLocation"] = static_cast<int64_t>(tailLocation);
  obj["fileFormat"] = static_cast<int64_t>(fileFormat);
  if (fileSchema) {
    obj["fileSchema"] = fileSchema->serialize();
  }
  obj["serDeOptions"] = ISerializable::serialize(serDeOptions);
  obj["footerEstimatedSize"] = static_cast<int64_t>(footerEstimatedSize);
  obj["filePreloadThreshold"] = static_cast<int64_t>(filePreloadThreshold);
  obj["fileColumnNamesReadAsLowerCase"] = fileColumnNamesReadAsLowerCase;
  obj["useColumnNamesForColumnMapping"] = useColumnNamesForColumnMapping_;
  obj["useNestedColumnNamesForColumnMapping"] =
      useNestedColumnNamesForColumnMapping_;
  // Note: decrypterFactory_ and ioExecutor_ are not serialized
  return obj;
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
std::shared_ptr<WriterOptions> WriterOptions::create(
    const folly::dynamic& obj) {
  auto opts = std::make_shared<WriterOptions>();

  // 1) schema
  if (const auto* p = obj.get_ptr("schema")) {
    opts->schema = ISerializable::deserialize<bolt::Type>(*p);
  }

  // 2) compressionKind
  if (const auto* p = obj.get_ptr("compressionKind")) {
    opts->compressionKind =
        static_cast<bolt::common::CompressionKind>(p->asInt());
  }

  // 3) serdeParameters
  if (const auto* p = obj.get_ptr("serdeParameters")) {
    opts->serdeParameters.clear();
    for (auto& kv : p->items()) {
      opts->serdeParameters.emplace(kv.first.asString(), kv.second.asString());
    }
  }

  // 4) maxStripeSize
  if (const auto* p = obj.get_ptr("maxStripeSize")) {
    opts->maxStripeSize = static_cast<uint64_t>(p->asInt());
  }

  // 5) arrowBridgeTimestampUnit
  if (const auto* p = obj.get_ptr("arrowBridgeTimestampUnit")) {
    opts->arrowBridgeTimestampUnit = static_cast<uint8_t>(p->asInt());
  }

  // 6) zlibCompressionLevel
  if (const auto* p = obj.get_ptr("zlibCompressionLevel")) {
    opts->zlibCompressionLevel = static_cast<uint8_t>(p->asInt());
  }

  // 7) spillConfig
  if (const auto* p = obj.get_ptr("spillConfig")) {
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
  registry.Register("WriterOptions", WriterOptions::create);
}

ReaderOptions ReaderOptions::create(
    const folly::dynamic& obj,
    bolt::memory::MemoryPool* pool) {
  auto baseOptions = io::ReaderOptions::create(obj, pool);
  ReaderOptions options(pool);
  static_cast<io::ReaderOptions&>(options) = baseOptions;

  if (const auto* p = obj.get_ptr("tailLocation")) {
    if (p->isNumber()) {
      options.tailLocation = static_cast<uint64_t>(p->asInt());
    }
  }
  if (const auto* p = obj.get_ptr("fileFormat")) {
    if (p->isNumber()) {
      options.fileFormat = static_cast<FileFormat>(p->asInt());
    }
  }
  if (const auto* p = obj.get_ptr("fileSchema")) {
    options.fileSchema = std::dynamic_pointer_cast<const bolt::RowType>(
        bolt::ISerializable::deserialize<bolt::Type>(*p));
  }
  if (const auto* p = obj.get_ptr("serDeOptions")) {
    options.serDeOptions = SerDeOptions::deserialize(*p);
  }
  if (const auto* p = obj.get_ptr("footerEstimatedSize")) {
    if (p->isNumber()) {
      options.footerEstimatedSize = static_cast<uint64_t>(p->asInt());
    }
  }
  if (const auto* p = obj.get_ptr("filePreloadThreshold")) {
    if (p->isNumber()) {
      options.filePreloadThreshold = static_cast<uint64_t>(p->asInt());
    }
  }
  if (const auto* p = obj.get_ptr("fileColumnNamesReadAsLowerCase")) {
    if (p->isBool()) {
      options.fileColumnNamesReadAsLowerCase = p->asBool();
    }
  }
  if (const auto* p = obj.get_ptr("useColumnNamesForColumnMapping")) {
    if (p->isBool()) {
      options.useColumnNamesForColumnMapping_ = p->asBool();
    }
  }
  if (const auto* p = obj.get_ptr("useNestedColumnNamesForColumnMapping")) {
    if (p->isBool()) {
      options.useNestedColumnNamesForColumnMapping_ = p->asBool();
    }
  }
  return options;
}

void ReaderOptions::registerSerDe() {
  bolt::Type::registerSerDe();
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register(
      "ReaderOptions",
      [](const folly::dynamic& obj)
          -> std::shared_ptr<const bolt::ISerializable> {
        // MemoryPool is not serialized - this will be a dummy, but host should
        // replace it before use.
        // In practice, deserialize() should be called directly with a real
        // pool.
        bolt::memory::MemoryPool* dummyPool = nullptr;
        auto options = ReaderOptions::create(obj, dummyPool);
        return std::make_shared<ReaderOptions>(std::move(options));
      });
}

} // namespace bytedance::bolt::dwio::common

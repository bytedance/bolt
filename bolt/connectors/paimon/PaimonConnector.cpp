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

#include "bolt/connectors/paimon/PaimonConnector.h"
#include <paimon/factories/factory.h>
#include <paimon/factories/factory_creator.h>
#include <paimon/format/file_format_factory.h>
#include <paimon/fs/file_system_factory.h>
#include <algorithm>
#include "bolt/connectors/paimon/PaimonBoltHdfsFileSystem.h"
#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonDataSource.h"
#include "bolt/connectors/paimon/PaimonParquetReader.h"

// Forward declarations for paimon factories whose headers are not publicly
// exposed by their respective conan components. Explicit registration here
// avoids linker stripping of REGISTER_PAIMON_FACTORY's static constructors.
namespace paimon::avro {
class AvroFileFormatFactory : public ::paimon::FileFormatFactory {
 public:
  static const char IDENTIFIER[];

  const char* Identifier() const override;

  ::paimon::Result<std::unique_ptr<::paimon::FileFormat>> Create(
      const std::map<std::string, std::string>& options) const override;
};
} // namespace paimon::avro

namespace paimon {

class LocalFileSystemFactory : public ::paimon::FileSystemFactory {
 public:
  static const char IDENTIFIER[];

  const char* Identifier() const override;

  ::paimon::Result<std::unique_ptr<::paimon::FileSystem>> Create(
      const std::string& path,
      const std::map<std::string, std::string>& options) const override;
};
} // namespace paimon

namespace bytedance::bolt::connector::paimon {

namespace {

void ensureAvroFormatFactoryRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    auto* factory = new ::paimon::avro::AvroFileFormatFactory;
    const auto& items =
        ::paimon::FactoryCreator::GetInstance()->GetRegisteredType();
    if (std::find(items.begin(), items.end(), factory->Identifier()) ==
        items.end()) {
      ::paimon::FactoryCreator::GetInstance()->Register(
          factory->Identifier(), factory);
    }
    LOG(INFO) << "[PAIMON] AvroFileFormatFactory registration complete";
  });
}

void ensureLocalFileSystemFactoryRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    auto* factory = new ::paimon::LocalFileSystemFactory;
    const auto& items =
        ::paimon::FactoryCreator::GetInstance()->GetRegisteredType();
    if (std::find(items.begin(), items.end(), factory->Identifier()) ==
        items.end()) {
      ::paimon::FactoryCreator::GetInstance()->Register(
          factory->Identifier(), factory);
    }
  });
}

} // namespace

std::unique_ptr<DataSource> PaimonConnector::createDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
        columnHandles,
    std::shared_ptr<ConnectorQueryCtx> queryCtx,
    const core::QueryConfig& queryConfig) {
  auto paimonConfig = std::make_shared<PaimonConfig>(config_);
  return std::make_unique<PaimonDataSource>(
      outputType,
      tableHandle,
      columnHandles,
      queryCtx,
      queryConfig,
      paimonConfig);
}

PaimonConnectorFactory::PaimonConnectorFactory()
    : ConnectorFactory(kPaimonConnectorName) {
  LOG(INFO)
      << "[PAIMON] PaimonConnectorFactory constructed, registering factories";
  // Register paimon factories (parquet format, avro format, local filesystem,
  // HDFS filesystem) so they are available when paimon-cpp resolves
  // format identifiers. Using explicit calls rather than relying on
  // REGISTER_PAIMON_FACTORY's static constructors, which the linker may strip
  // from paimon's format libraries.
  EnsurePaimonParquetFormatRegistered();
  ensureAvroFormatFactoryRegistered();
  ensureLocalFileSystemFactoryRegistered();
#ifdef BOLT_ENABLE_HDFS
  EnsurePaimonBoltHdfsFileSystemRegistered();
#endif
}

} // namespace bytedance::bolt::connector::paimon

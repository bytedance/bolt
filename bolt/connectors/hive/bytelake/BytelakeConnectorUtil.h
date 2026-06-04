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

#pragma once
#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/FileHandle.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/bytelake/BytelakeConnectorSplit.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Reader.h"

namespace bytedance::bolt::connector::hive {

class HiveColumnHandle;
class HiveConfig;
struct HiveConnectorSplit;

std::vector<std::string> splitByDelimiter(const std::string& s, char delimiter);

std::vector<std::string> getBytelakePrimaryKeys(
    const std::string& tableName,
    const std::unordered_map<std::string, std::string>& tableParameters);

std::vector<std::string> getBytelakePrecombineFields(
    const std::unordered_map<std::string, std::string>& tableParameters);

std::string getBytelakeDeletedField(
    const std::unordered_map<std::string, std::string>& tableParameters);

} // namespace bytedance::bolt::connector::hive

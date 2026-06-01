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
#include <sstream>
#include "bolt/connectors/hive/bytelake/BytelakeConnectorUtil.h"
#include "bolt/connectors/hive/bytelake/BytelakeConstants.h"

namespace bytedance::bolt::connector::hive {

std::vector<std::string> splitByDelimiter(const std::string& s, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::stringstream ss(s);

  while (std::getline(ss, token, delimiter)) {
    tokens.push_back(token);
  }

  return tokens;
}

std::vector<std::string> getBytelakePrimaryKeys(
    const std::string& tableName,
    const std::unordered_map<std::string, std::string>& tableParameters) {
  auto primaryKeyIter = tableParameters.find(connector::bytelake::kPrimaryKey);
  if (primaryKeyIter == tableParameters.end()) {
    throw std::invalid_argument(fmt::format(
        "No primary key in table {}", tableName));
  }

  std::string primaryKeys = primaryKeyIter->second;
  return splitByDelimiter(primaryKeys, ',');
}

std::vector<std::string> getBytelakePrecombineFields(
    const std::unordered_map<std::string, std::string>& tableParameters) {
  auto precombineFieldIter =
      tableParameters.find(connector::bytelake::kPrecombineKey);

  std::vector<std::string> precombineFields;
  if (precombineFieldIter != tableParameters.end()) {
    precombineFields = splitByDelimiter(precombineFieldIter->second, ',');
  }

  std::string commitTimeField = connector::bytelake::kMetaCommitTime;
  precombineFields.push_back(commitTimeField);

  return precombineFields;
}

std::string getBytelakeDeletedField(
    const std::unordered_map<std::string, std::string>& tableParameters) {
  std::string deletedField = connector::bytelake::kHoodieIsDeleted;
  return deletedField;
}

} // namespace bytedance::bolt::connector::hive

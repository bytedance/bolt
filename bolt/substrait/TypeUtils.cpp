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

#include "bolt/substrait/TypeUtils.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::substrait {
std::vector<std::string_view> getTypesFromCompoundName(
    std::string_view compoundName) {
  // CompoundName is like ARRAY<BIGINT> or MAP<BIGINT,DOUBLE>
  // or ROW<BIGINT,ROW<DOUBLE,BIGINT>,ROW<DOUBLE,BIGINT>>
  // the position of then delimiter is where the number of leftAngleBracket
  // equals rightAngleBracket need to split.
  std::vector<std::string_view> types;
  std::vector<int> angleBracketNumEqualPos;
  auto leftAngleBracketPos = compoundName.find("<");
  auto rightAngleBracketPos = compoundName.rfind(">");
  auto typesName = compoundName.substr(
      leftAngleBracketPos + 1, rightAngleBracketPos - leftAngleBracketPos - 1);
  int leftAngleBracketNum = 0;
  int rightAngleBracketNum = 0;
  for (auto index = 0; index < typesName.length(); index++) {
    if (typesName[index] == '<') {
      leftAngleBracketNum++;
    }
    if (typesName[index] == '>') {
      rightAngleBracketNum++;
    }
    if (typesName[index] == ',' &&
        rightAngleBracketNum == leftAngleBracketNum) {
      angleBracketNumEqualPos.push_back(index);
    }
  }
  int startPos = 0;
  for (auto delimeterPos : angleBracketNumEqualPos) {
    types.emplace_back(typesName.substr(startPos, delimeterPos - startPos));
    startPos = delimeterPos + 1;
  }
  types.emplace_back(std::string_view(
      typesName.data() + startPos, typesName.length() - startPos));
  return types;
}

// TODO Refactor using Bison.
std::string_view getNameBeforeDelimiter(
    const std::string& compoundName,
    const std::string& delimiter) {
  std::size_t pos = compoundName.find(delimiter);
  if (pos == std::string::npos) {
    return compoundName;
  }
  return std::string_view(compoundName.data(), pos);
}

dwio::common::FileFormat toBoltFileFormat(
    const ::substrait::ReadRel::LocalFiles::FileOrFiles& file) {
  using SubstraitFileFormatCase =
      ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;
  switch (file.file_format_case()) {
    case SubstraitFileFormatCase::kOrc:
      return dwio::common::FileFormat::ORC;
    case SubstraitFileFormatCase::kParquet:
      return dwio::common::FileFormat::PARQUET;
    case SubstraitFileFormatCase::kDwrf:
      return dwio::common::FileFormat::DWRF;
    case SubstraitFileFormatCase::kText:
      return dwio::common::FileFormat::TEXT;
    case SubstraitFileFormatCase::kArrow:
      return dwio::common::FileFormat::UNKNOWN;
    case SubstraitFileFormatCase::kExtension:
      return dwio::common::FileFormat::UNKNOWN;
    case SubstraitFileFormatCase::FILE_FORMAT_NOT_SET:
    default:
      return dwio::common::FileFormat::UNKNOWN;
  }
}

::substrait::ReadRel::LocalFiles::FileOrFiles toSubstraitFileFormat(
    dwio::common::FileFormat fmt) {
  ::substrait::ReadRel::LocalFiles::FileOrFiles file;
  switch (fmt) {
    case dwio::common::FileFormat::ORC:
      file.mutable_orc();
      break;
    case dwio::common::FileFormat::PARQUET:
      file.mutable_parquet();
      break;
    case dwio::common::FileFormat::DWRF:
      file.mutable_dwrf();
      break;
    case dwio::common::FileFormat::TEXT:
      file.mutable_text();
      break;
    default:
      // Leave unset for UNKNOWN and other unsupported mappings.
      break;
  }
  return file;
}

} // namespace bytedance::bolt::substrait

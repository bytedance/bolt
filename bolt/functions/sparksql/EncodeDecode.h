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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "bolt/expression/VectorFunction.h"

namespace bytedance::bolt::functions::sparksql {

/// encode(str, charset) -> varbinary
///
/// Encodes 'str' using the named charset, matching Spark's Encode expression.
/// Spark documents US-ASCII, ISO-8859-1, UTF-8, UTF-16BE, UTF-16LE and UTF-16.
///
/// Characters that the target charset cannot represent become '?', matching
/// Java's String.getBytes(Charset). Note this differs from ICU's own default
/// substitution byte (0x1A), so the substitution is set explicitly.
std::shared_ptr<exec::VectorFunction> makeEncode(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> encodeSignatures();

/// decode(bin, charset) -> varchar
///
/// Decodes 'bin' from the named charset, matching Spark's StringDecode.
/// Byte sequences that are not valid in the source charset become U+FFFD,
/// matching Java's String(byte[], Charset).
std::shared_ptr<exec::VectorFunction> makeDecode(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> decodeSignatures();

} // namespace bytedance::bolt::functions::sparksql

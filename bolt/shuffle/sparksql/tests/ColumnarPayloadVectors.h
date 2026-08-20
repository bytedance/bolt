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

/// RowVector interface to the ColumnarPayload reference implementation.
///
/// This is the only file in the reference implementation that depends on the
/// vector library. Everything below it (ColumnarPayloadFormat,
/// ColumnarPayloadGenerator, ColumnarPayloadValidator) works on plain arrays
/// and can be built and exercised without the engine, which is what lets it
/// serve as an independent oracle for the engine's own Writer and Reader.
///
/// Only flat children are supported. An encoded child is rejected rather than
/// decoded, so a caller holding a dictionary or constant vector must flatten
/// it first and stay aware that it did.

#pragma once

#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/tests/ColumnarPayloadGenerator.h"
#include "bolt/shuffle/sparksql/tests/ColumnarPayloadValidator.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql::test {

/// Maps a Bolt type onto the format's physical type. Returns false for a type
/// the format does not carry.
///
/// DATE needs care: it is not a TypeKind of its own but an IntegerType
/// subclass, so a plain TypeKind switch would report it as kInteger and the
/// rebuilt vector would come back with the wrong type.
bool toPhysicalType(const TypePtr& type, PhysicalType& out);

/// The Bolt type a physical type is rebuilt as.
TypePtr toBoltType(PhysicalType type);

/// The schema of a RowVector in the form the generator and validator expect.
bool schemaOf(
    const RowVectorPtr& vector,
    std::vector<PhysicalType>& out,
    std::string& error);

/// Copies a RowVector into the flat staging arrays. Fails when a child is not
/// flat, or carries a type the format does not support.
bool toFlatTable(
    const RowVectorPtr& vector,
    FlatTable& out,
    std::string& error);

/// Rebuilds a RowVector from decoded values. `rowType` fixes the column names
/// and the exact types, which matters for DATE.
RowVectorPtr toRowVector(
    const FlatTable& table,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool);

/// Encodes a RowVector directly.
bool generatePayload(
    ColumnarPayloadGenerator& generator,
    const RowVectorPtr& input,
    GeneratedPayload& out,
    std::string& error);

/// Validates a payload and rebuilds the RowVector it carries. `rowType` must
/// be the type the payload was produced from; the format stores no schema.
/// `result.decoded` is left null when parsing failed early.
struct VectorValidationResult {
  ValidationResult result;
  RowVectorPtr decoded;
};

VectorValidationResult validatePayload(
    ColumnarPayloadValidator& validator,
    const std::vector<uint8_t>& payload,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool);

/// One corpus entry as a RowVector.
struct NamedVector {
  const char* name;
  RowVectorPtr vector;
};

/// boundaryCorpus() rendered as RowVectors. Column names are c0, c1, ...
std::vector<NamedVector> boundaryVectorCorpus(memory::MemoryPool* pool);

} // namespace bytedance::bolt::shuffle::sparksql::test

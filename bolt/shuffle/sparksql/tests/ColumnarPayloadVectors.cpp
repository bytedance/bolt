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

#include "bolt/shuffle/sparksql/tests/ColumnarPayloadVectors.h"

#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

template <typename T>
const FlatVector<T>* asFlat(const VectorPtr& child) {
  return child->template asFlatVector<T>();
}

template <typename T>
FlatVectorPtr<T>
newFlat(const TypePtr& type, vector_size_t size, memory::MemoryPool* pool) {
  return BaseVector::create<FlatVector<T>>(type, size, pool);
}

/// Reads an integral child, widening to the int64_t the encoder works on.
template <typename T>
void readIntegral(
    const VectorPtr& child,
    vector_size_t size,
    FlatColumn& out) {
  const auto* flat = asFlat<T>(child);
  for (vector_size_t row = 0; row < size; ++row) {
    const bool isNull = flat->isNullAt(row);
    out.isNull[static_cast<size_t>(row)] = isNull;
    if (!isNull) {
      out.intValues.push_back(static_cast<int64_t>(flat->valueAt(row)));
    }
  }
}

template <typename T>
void readFloating(
    const VectorPtr& child,
    vector_size_t size,
    FlatColumn& out) {
  const auto* flat = asFlat<T>(child);
  for (vector_size_t row = 0; row < size; ++row) {
    const bool isNull = flat->isNullAt(row);
    out.isNull[static_cast<size_t>(row)] = isNull;
    if (!isNull) {
      out.doubleValues.push_back(static_cast<double>(flat->valueAt(row)));
    }
  }
}

template <typename T>
VectorPtr writeIntegral(
    const TypePtr& type,
    const FlatColumn& column,
    memory::MemoryPool* pool) {
  const auto size = static_cast<vector_size_t>(column.isNull.size());
  auto flat = newFlat<T>(type, size, pool);
  size_t next = 0;
  for (vector_size_t row = 0; row < size; ++row) {
    if (column.isNull[static_cast<size_t>(row)]) {
      flat->setNull(row, true);
    } else {
      flat->set(row, static_cast<T>(column.intValues[next++]));
    }
  }
  return flat;
}

template <typename T>
VectorPtr writeFloating(
    const TypePtr& type,
    const FlatColumn& column,
    memory::MemoryPool* pool) {
  const auto size = static_cast<vector_size_t>(column.isNull.size());
  auto flat = newFlat<T>(type, size, pool);
  size_t next = 0;
  for (vector_size_t row = 0; row < size; ++row) {
    if (column.isNull[static_cast<size_t>(row)]) {
      flat->setNull(row, true);
    } else {
      flat->set(row, static_cast<T>(column.doubleValues[next++]));
    }
  }
  return flat;
}

} // namespace

bool toPhysicalType(const TypePtr& type, PhysicalType& out) {
  if (type == nullptr) {
    return false;
  }
  // DATE is an IntegerType subclass rather than a TypeKind, so it has to be
  // recognised before the switch or it would come back as kInteger and the
  // rebuilt vector would carry the wrong type.
  if (type->isDate()) {
    out = PhysicalType::kDate;
    return true;
  }
  switch (type->kind()) {
    case TypeKind::TINYINT:
      out = PhysicalType::kTinyInt;
      return true;
    case TypeKind::SMALLINT:
      out = PhysicalType::kSmallInt;
      return true;
    case TypeKind::INTEGER:
      out = PhysicalType::kInteger;
      return true;
    case TypeKind::BIGINT:
      out = PhysicalType::kBigint;
      return true;
    case TypeKind::REAL:
      out = PhysicalType::kFloat;
      return true;
    case TypeKind::DOUBLE:
      out = PhysicalType::kDouble;
      return true;
    case TypeKind::VARCHAR:
      out = PhysicalType::kString;
      return true;
    default:
      return false;
  }
}

TypePtr toBoltType(PhysicalType type) {
  switch (type) {
    case PhysicalType::kTinyInt:
      return TINYINT();
    case PhysicalType::kSmallInt:
      return SMALLINT();
    case PhysicalType::kInteger:
      return INTEGER();
    case PhysicalType::kBigint:
      return BIGINT();
    case PhysicalType::kDate:
      return DATE();
    case PhysicalType::kFloat:
      return REAL();
    case PhysicalType::kDouble:
      return DOUBLE();
    case PhysicalType::kString:
      return VARCHAR();
  }
  return nullptr;
}

bool schemaOf(
    const RowVectorPtr& vector,
    std::vector<PhysicalType>& out,
    std::string& error) {
  if (vector == nullptr) {
    error = "row vector is null";
    return false;
  }
  const auto rowType = asRowType(vector->type());
  if (rowType == nullptr) {
    error = "vector is not a row";
    return false;
  }
  out.clear();
  out.reserve(rowType->size());
  for (uint32_t child = 0; child < rowType->size(); ++child) {
    PhysicalType physical{};
    if (!toPhysicalType(rowType->childAt(child), physical)) {
      error = std::string("column ") + std::to_string(child) + " has type " +
          rowType->childAt(child)->toString() +
          ", which the format does not carry";
      return false;
    }
    out.push_back(physical);
  }
  return true;
}

bool toFlatTable(
    const RowVectorPtr& vector,
    FlatTable& out,
    std::string& error) {
  std::vector<PhysicalType> schema;
  if (!schemaOf(vector, schema, error)) {
    return false;
  }

  const auto size = vector->size();
  out.rowCount = static_cast<uint32_t>(size);
  out.columns.assign(schema.size(), FlatColumn{});

  for (size_t index = 0; index < schema.size(); ++index) {
    const auto& child = vector->childAt(index);
    if (child == nullptr) {
      error = "column " + std::to_string(index) + " is null";
      return false;
    }
    // Only flat children are handled; a caller holding an encoded vector has
    // to flatten it and stay aware that it did.
    if (child->encoding() != VectorEncoding::Simple::FLAT) {
      error = "column " + std::to_string(index) + " is " +
          std::string(VectorEncoding::mapSimpleToName(child->encoding())) +
          ", not flat";
      return false;
    }
    if (child->size() < size) {
      error = "column " + std::to_string(index) + " is shorter than the row";
      return false;
    }

    auto& column = out.columns[index];
    column.type = schema[index];
    column.isNull.assign(static_cast<size_t>(size), false);

    switch (schema[index]) {
      case PhysicalType::kTinyInt:
        readIntegral<int8_t>(child, size, column);
        break;
      case PhysicalType::kSmallInt:
        readIntegral<int16_t>(child, size, column);
        break;
      case PhysicalType::kInteger:
      case PhysicalType::kDate:
        readIntegral<int32_t>(child, size, column);
        break;
      case PhysicalType::kBigint:
        readIntegral<int64_t>(child, size, column);
        break;
      case PhysicalType::kFloat:
        readFloating<float>(child, size, column);
        break;
      case PhysicalType::kDouble:
        readFloating<double>(child, size, column);
        break;
      case PhysicalType::kString: {
        const auto* flat = asFlat<StringView>(child);
        for (vector_size_t row = 0; row < size; ++row) {
          const bool isNull = flat->isNullAt(row);
          column.isNull[static_cast<size_t>(row)] = isNull;
          if (!isNull) {
            const auto view = flat->valueAt(row);
            column.stringValues.emplace_back(view.data(), view.size());
          }
        }
        break;
      }
    }
  }
  return true;
}

RowVectorPtr toRowVector(
    const FlatTable& table,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  // A parse that stopped early leaves columns whose value arrays are shorter
  // than their null bitmaps say. Rebuilding from those would read past the
  // end of the arrays, so refuse instead.
  if (table.columns.size() != rowType->size()) {
    return nullptr;
  }
  for (const auto& column : table.columns) {
    if (column.isNull.size() != table.rowCount) {
      return nullptr;
    }
    size_t values = 0;
    switch (column.type) {
      case PhysicalType::kFloat:
      case PhysicalType::kDouble:
        values = column.doubleValues.size();
        break;
      case PhysicalType::kString:
        values = column.stringValues.size();
        break;
      default:
        values = column.intValues.size();
        break;
    }
    if (values != column.nonNullCount()) {
      return nullptr;
    }
  }

  std::vector<VectorPtr> children;
  children.reserve(table.columns.size());

  for (size_t index = 0; index < table.columns.size(); ++index) {
    const auto& column = table.columns[index];
    const auto type = rowType->childAt(index);
    const auto size = static_cast<vector_size_t>(column.isNull.size());

    switch (column.type) {
      case PhysicalType::kTinyInt:
        children.push_back(writeIntegral<int8_t>(type, column, pool));
        break;
      case PhysicalType::kSmallInt:
        children.push_back(writeIntegral<int16_t>(type, column, pool));
        break;
      case PhysicalType::kInteger:
      case PhysicalType::kDate:
        children.push_back(writeIntegral<int32_t>(type, column, pool));
        break;
      case PhysicalType::kBigint:
        children.push_back(writeIntegral<int64_t>(type, column, pool));
        break;
      case PhysicalType::kFloat:
        children.push_back(writeFloating<float>(type, column, pool));
        break;
      case PhysicalType::kDouble:
        children.push_back(writeFloating<double>(type, column, pool));
        break;
      case PhysicalType::kString: {
        auto flat = newFlat<StringView>(type, size, pool);
        size_t next = 0;
        for (vector_size_t row = 0; row < size; ++row) {
          if (column.isNull[static_cast<size_t>(row)]) {
            flat->setNull(row, true);
          } else {
            const auto& value = column.stringValues[next++];
            flat->set(row, StringView(value.data(), value.size()));
          }
        }
        children.push_back(flat);
        break;
      }
    }
  }

  return std::make_shared<RowVector>(
      pool,
      rowType,
      BufferPtr(nullptr),
      static_cast<size_t>(table.rowCount),
      std::move(children));
}

bool generatePayload(
    ColumnarPayloadGenerator& generator,
    const RowVectorPtr& input,
    GeneratedPayload& out,
    std::string& error) {
  FlatTable table;
  if (!toFlatTable(input, table, error)) {
    return false;
  }
  return generator.generate(table, out, error);
}

VectorValidationResult validatePayload(
    ColumnarPayloadValidator& validator,
    const std::vector<uint8_t>& payload,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  VectorValidationResult out;
  out.result = validator.validate(payload);
  // Only a clean parse leaves values that can be rebuilt. A failed one may
  // still have filled the null bitmaps, which is not enough, and the column
  // count alone is not a safe test for that.
  if (out.result.ok()) {
    out.decoded = toRowVector(out.result.decoded, rowType, pool);
  }
  return out;
}

std::vector<NamedVector> boundaryVectorCorpus(memory::MemoryPool* pool) {
  std::vector<NamedVector> corpus;
  for (const auto& entry : boundaryCorpus()) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(entry.table.columns.size());
    types.reserve(entry.table.columns.size());
    for (size_t index = 0; index < entry.table.columns.size(); ++index) {
      names.push_back("c" + std::to_string(index));
      types.push_back(toBoltType(entry.table.columns[index].type));
    }
    const auto rowType = ROW(std::move(names), std::move(types));
    corpus.push_back({entry.name, toRowVector(entry.table, rowType, pool)});
  }
  return corpus;
}

} // namespace bytedance::bolt::shuffle::sparksql::test

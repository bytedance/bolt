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

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include "bolt/type/Type.h"

using namespace ::bytedance::bolt;

namespace {

std::string serializeType(const std::shared_ptr<const Type>& type) {
  const auto& obj = type->serialize();
  return folly::json::serialize(obj, getSerializationOptions());
}

} // namespace

namespace bytedance::bolt::python {

void addDataTypeBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  // Add TypeKind enum.
  pybind11::enum_<TypeKind>(
      m, "TypeKind", pybind11::module_local(asModuleLocalDefinitions))
      .value("BOOLEAN", TypeKind::BOOLEAN)
      .value("TINYINT", TypeKind::TINYINT)
      .value("SMALLINT", TypeKind::SMALLINT)
      .value("INTEGER", TypeKind::INTEGER)
      .value("BIGINT", TypeKind::BIGINT)
      .value("REAL", TypeKind::REAL)
      .value("DOUBLE", TypeKind::DOUBLE)
      .value("VARCHAR", TypeKind::VARCHAR)
      .value("VARBINARY", TypeKind::VARBINARY)
      .value("TIMESTAMP", TypeKind::TIMESTAMP)
      .value("HUGEINT", TypeKind::HUGEINT)
      .value("ARRAY", TypeKind::ARRAY)
      .value("MAP", TypeKind::MAP)
      .value("ROW", TypeKind::ROW)
      .value("UNKNOWN", TypeKind::UNKNOWN)
      .value("FUNCTION", TypeKind::FUNCTION)
      .value("OPAQUE", TypeKind::OPAQUE)
      .value("INVALID", TypeKind::INVALID)
      .export_values();

  // Create BoltType bound to Type.
  pybind11::class_<Type, std::shared_ptr<Type>> type(
      m, "BoltType", pybind11::module_local(asModuleLocalDefinitions));

  // Adding all the derived types of Type here.
  pybind11::class_<BooleanType, Type, std::shared_ptr<BooleanType>> booleanType(
      m, "BooleanType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<IntegerType, Type, std::shared_ptr<IntegerType>> integerType(
      m, "IntegerType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<DateType, IntegerType, std::shared_ptr<DateType>> dateType(
      m, "DateType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<BigintType, Type, std::shared_ptr<BigintType>> bigintType(
      m, "BigintType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<HugeintType, Type, std::shared_ptr<HugeintType>> hugeintType(
      m, "HugeintType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<SmallintType, Type, std::shared_ptr<SmallintType>>
      smallintType(
          m, "SmallintType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<TinyintType, Type, std::shared_ptr<TinyintType>> tinyintType(
      m, "TinyintType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<RealType, Type, std::shared_ptr<RealType>> realType(
      m, "RealType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<DoubleType, Type, std::shared_ptr<DoubleType>> doubleType(
      m, "DoubleType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<TimestampType, Type, std::shared_ptr<TimestampType>>
      timestampType(
          m, "TimestampType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<VarcharType, Type, std::shared_ptr<VarcharType>> varcharType(
      m, "VarcharType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<VarbinaryType, Type, std::shared_ptr<VarbinaryType>>
      varbinaryType(
          m, "VarbinaryType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<ArrayType, Type, std::shared_ptr<ArrayType>> arrayType(
      m, "ArrayType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<MapType, Type, std::shared_ptr<MapType>> mapType(
      m, "MapType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<RowType, Type, std::shared_ptr<RowType>> rowType(
      m, "RowType", pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<ShortDecimalType, Type, std::shared_ptr<ShortDecimalType>>
      shortDecimalType(
          m,
          "ShortDecimalType",
          pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<LongDecimalType, Type, std::shared_ptr<LongDecimalType>>
      longDecimalType(
          m,
          "LongDecimalType",
          pybind11::module_local(asModuleLocalDefinitions));
  pybind11::class_<UnknownType, Type, std::shared_ptr<UnknownType>> unknownType(
      m, "UnknownType", pybind11::module_local(asModuleLocalDefinitions));

  // Basic operations on Type.
  type.def("__str__", &Type::toString);
  type.def(
      "cppSizeInBytes", &Type::cppSizeInBytes, "Return the C++ size in bytes");
  type.def(
      "isFixedWidth", &Type::isFixedWidth, "Check if the type is fixed width");
  type.def(
      "isPrimitiveType",
      &Type::isPrimitiveType,
      "Check if the type is a primitive type");
  type.def(
      "isDecimal", &Type::isDecimal, "Check if the type is a decimal type");
  type.def("name", &Type::name, "Returns the name of the type");
  type.def("kind", &Type::kind, "Returns the kind of the type");
  type.def("serialize", &serializeType, "Serializes the type as JSON");
  type.def("__eq__", &Type::equivalent);

  booleanType.def(pybind11::init());
  tinyintType.def(pybind11::init());
  smallintType.def(pybind11::init());
  integerType.def(pybind11::init());
  dateType.def(pybind11::init(
      []() { return std::const_pointer_cast<DateType>(DATE()); }));
  hugeintType.def(pybind11::init());
  bigintType.def(pybind11::init());
  realType.def(pybind11::init());
  doubleType.def(pybind11::init());
  varcharType.def(pybind11::init());
  varbinaryType.def(pybind11::init());
  timestampType.def(pybind11::init());
  unknownType.def(pybind11::init());
  arrayType.def(pybind11::init<std::shared_ptr<Type>>());
  arrayType.def(
      "elementType", &ArrayType::elementType, "Return the element type");
  mapType.def(pybind11::init<std::shared_ptr<Type>, std::shared_ptr<Type>>());
  mapType.def("keyType", &MapType::keyType, "Return the key type");
  mapType.def("valueType", &MapType::valueType, "Return the value type");

  rowType.def(pybind11::init<
              std::vector<std::string>,
              std::vector<std::shared_ptr<const Type>>>());
  rowType.def("size", &RowType::size, "Return the number of columns");
  rowType.def(
      "childAt",
      &RowType::childAt,
      "Return the type of the column at a given index",
      pybind11::arg("idx"));
  rowType.def(
      "findChild",
      [](const std::shared_ptr<RowType>& type, const std::string& name) {
        return type->findChild(name);
      },
      "Return the type of the column with the given name",
      pybind11::arg("name"));
  rowType.def(
      "getChildIdx",
      &RowType::getChildIdx,
      "Return the index of the column with the given name",
      pybind11::arg("name"));
  rowType.def(
      "nameOf",
      &RowType::nameOf,
      "Return the name of the column at the given index",
      pybind11::arg("idx"));
  rowType.def("names", &RowType::names, "Return the names of the columns");
  rowType.def("__len__", &RowType::size);
  rowType.def(
      "__getitem__",
      [](const RowType& r, size_t i) {
        // Perform bounds checking
        if (i >= r.size()) {
          throw pybind11::index_error("RowType index out of range");
        }
        // Assuming RowType::childAt takes uint32_t or similar,
        // size_t should be compatible or pybind11 handles conversion.
        // Bolt typically uses uint32_t for indices.
        return pybind11::make_tuple(
            r.nameOf(static_cast<uint32_t>(i)),
            r.childAt(static_cast<uint32_t>(i)));
      },
      pybind11::arg("index"),
      "Return the child type at the given index (supports iteration)");
  shortDecimalType.def(
      pybind11::init<int32_t, int32_t>(),
      "Construct a ShortDecimalType",
      pybind11::arg("precision"),
      pybind11::arg("scale"));
  shortDecimalType.def(
      "precision",
      &ShortDecimalType::precision,
      "Return the precision of the type");
  shortDecimalType.def(
      "scale", &ShortDecimalType::scale, "Return the scale of the type");
  longDecimalType.def(
      pybind11::init<int32_t, int32_t>(),
      "Construct a LongDecimalType",
      pybind11::arg("precision"),
      pybind11::arg("scale"));
  longDecimalType.def(
      "precision",
      &LongDecimalType::precision,
      "Return the precision of the type");
  longDecimalType.def(
      "scale", &LongDecimalType::scale, "Return the scale of the type");

  pybind11::class_<Timestamp> timestamp(
      m, "Timestamp", pybind11::module_local(asModuleLocalDefinitions));
  timestamp.def(
      pybind11::init<int64_t, uint64_t>(),
      "Create a timestamp",
      pybind11::arg("seconds"),
      pybind11::arg("nanos"));
  timestamp.def("now", &Timestamp::now);
  timestamp.def(
      "seconds", [](const Timestamp& self) { return self.getSeconds(); });
  timestamp.def("nanos", [](const Timestamp& self) { return self.getNanos(); });
  timestamp.def(
      "__str__", [](const Timestamp& self) { return self.toString(); });
  timestamp.def("__eq__", [](const Timestamp& self, const Timestamp& rhs) {
    return self == rhs;
  });
  timestamp.def("__ne__", [](const Timestamp& self, const Timestamp& rhs) {
    return self != rhs;
  });
  timestamp.def("__le__", [](const Timestamp& self, const Timestamp& rhs) {
    return self <= rhs;
  });
  timestamp.def("__ge__", [](const Timestamp& self, const Timestamp& rhs) {
    return self >= rhs;
  });
  timestamp.def("__lt__", [](const Timestamp& self, const Timestamp& rhs) {
    return self < rhs;
  });
  timestamp.def("__gt__", [](const Timestamp& self, const Timestamp& rhs) {
    return self > rhs;
  });
}

} // namespace bytedance::bolt::python

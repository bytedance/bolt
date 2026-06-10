/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

// Runtime helpers called from JIT-generated HashAggr extract IR. These are
// plain extern "C" functions resolved by the ORC JIT through the process
// global symbol table (see ThrustJITv2::Create / LoadLibraryPermanently), so
// they only need default visibility and to be linked into the host process.
// They were previously colocated in RowContainer.cpp purely because the
// jit_GetDecodedValue* helpers already lived there.

#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

extern "C" {

__attribute__((__visibility__("default"))) void jit_HashAggrResizeVector(
    char* vector,
    int32_t size) {
  reinterpret_cast<bytedance::bolt::BaseVector*>(vector)->resize(size);
}

__attribute__((__visibility__("default"))) void jit_HashAggrSetFlatI8(
    char* vector,
    int32_t row,
    int8_t value,
    int8_t isNull) {
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<int8_t>>();
  isNull ? flat->setNull(row, true) : flat->set(row, value);
}

__attribute__((__visibility__("default"))) void jit_HashAggrSetFlatI16(
    char* vector,
    int32_t row,
    int16_t value,
    int8_t isNull) {
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<int16_t>>();
  isNull ? flat->setNull(row, true) : flat->set(row, value);
}

__attribute__((__visibility__("default"))) void jit_HashAggrSetFlatI32(
    char* vector,
    int32_t row,
    int32_t value,
    int8_t isNull) {
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<int32_t>>();
  isNull ? flat->setNull(row, true) : flat->set(row, value);
}

__attribute__((__visibility__("default"))) void jit_HashAggrSetFlatI64(
    char* vector,
    int32_t row,
    int64_t value,
    int8_t isNull) {
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<int64_t>>();
  isNull ? flat->setNull(row, true) : flat->set(row, value);
}

__attribute__((__visibility__("default"))) void jit_HashAggrSetFlatFloat(
    char* vector,
    int32_t row,
    float value,
    int8_t isNull) {
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<float>>();
  isNull ? flat->setNull(row, true) : flat->set(row, value);
}

__attribute__((__visibility__("default"))) void jit_HashAggrSetFlatDouble(
    char* vector,
    int32_t row,
    double value,
    int8_t isNull) {
  auto* flat = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                   ->as<bytedance::bolt::FlatVector<double>>();
  isNull ? flat->setNull(row, true) : flat->set(row, value);
}

__attribute__((__visibility__("default"))) void jit_HashAggrSetPartialAvgDouble(
    char* vector,
    int32_t row,
    double sum,
    int64_t count,
    int8_t isNull) {
  auto* rowVector = reinterpret_cast<bytedance::bolt::BaseVector*>(vector)
                        ->as<bytedance::bolt::RowVector>();
  auto* sumVector = rowVector->childAt(0)->asFlatVector<double>();
  auto* countVector = rowVector->childAt(1)->asFlatVector<int64_t>();
  if (isNull) {
    rowVector->setNull(row, true);
    return;
  }
  rowVector->setNull(row, false);
  sumVector->set(row, sum);
  countVector->set(row, count);
}

} // extern "C"

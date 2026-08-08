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

#include "bolt/vector/FlatVector.h"

extern "C" {

__attribute__((__visibility__("default"))) void jit_HashAggrResizeVector(
    char* vector,
    int32_t size) {
  reinterpret_cast<bytedance::bolt::BaseVector*>(vector)->resize(size);
}

} // extern "C"

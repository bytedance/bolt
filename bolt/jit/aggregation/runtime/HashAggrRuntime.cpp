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

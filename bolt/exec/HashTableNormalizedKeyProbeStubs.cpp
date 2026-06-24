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
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/HashTable.h"

namespace bytedance::bolt::exec {

// Linked on non-aarch64 so HashTable.cpp can reference SVE symbols. Runtime
// uses kScalar/kNativeBolt only; these paths are not taken in normal operation.

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::groupNormalizedKeyProbeSVE(HashLookup& lookup) {
  groupNormalizedKeyProbeScalar(lookup);
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::insertForGroupBySve(
    char** /*groups*/,
    uint64_t* /*hashes*/,
    int32_t /*numGroups*/) {
  BOLT_FAIL(
      "SVE normalized-key insertForGroupBy requires aarch64 with SVE support");
}

template void HashTable<true>::groupNormalizedKeyProbeSVE(HashLookup&);
template void HashTable<false>::groupNormalizedKeyProbeSVE(HashLookup&);
template void HashTable<true>::insertForGroupBySve(char**, uint64_t*, int32_t);
template void HashTable<false>::insertForGroupBySve(char**, uint64_t*, int32_t);

} // namespace bytedance::bolt::exec

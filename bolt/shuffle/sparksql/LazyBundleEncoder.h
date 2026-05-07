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

#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql {

/// Fused shuffle-writer helper: encodes any raw complex children with
/// CompactRow and packs them into the lazy bundle wire in a single pass
/// through the bundle arena. Complex children that arrive already encoded
/// as LazyComplexVector are passed through (their bytes are memcpy'd into
/// the bundle without re-encoding). Non-complex children flow through at
/// their collapsed positions.
///
/// The output RowVector has the same wire shape as the non-fused path
/// (`toLazyBundleWireRowVector`), so the shuffle reader (`fromLazyBundle
/// WireRowVector`) works unchanged. The fusion saves the intermediate
/// per-column arena + the bundle-pack memcpy, giving one linear write
/// pass through the bundle memory instead of two.
///
/// Returns `input` unchanged when the codec is inactive or when the input
/// has no complex children.
RowVectorPtr encodeAndBundleLazyWireRowVector(
    const RowVectorPtr& input,
    memory::MemoryPool* pool);

} // namespace bytedance::bolt::shuffle::sparksql

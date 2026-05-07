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
#include <string_view>
#include <unordered_set>

#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/LazyComplexVector.h"
#include "bolt/vector/SelectivityVector.h"

namespace bytedance::bolt {

class LazyComplexCodec {
 public:
  virtual ~LazyComplexCodec() = default;

  virtual std::string_view name() const = 0;

  virtual std::shared_ptr<LazyComplexVector> encode(
      const VectorPtr& input,
      memory::MemoryPool* pool) const = 0;

  virtual VectorPtr decode(
      const LazyComplexVector& lazy,
      const SelectivityVector& rows,
      memory::MemoryPool* pool) const = 0;

  static void registerCodec(std::unique_ptr<LazyComplexCodec> codec);

  static void setActiveFormat(std::string_view name);
  static const LazyComplexCodec* activeCodec();
};

std::shared_ptr<LazyComplexVector> encodeToLazy(
    const VectorPtr& input,
    memory::MemoryPool* pool,
    const LazyComplexCodec& codec);

/// Returns a RowVector in which every top-level `LazyComplexVector` child has
/// been decoded back to its original complex-type representation (ArrayVector,
/// MapVector, or RowVector). Children that are not lazy-encoded are returned
/// unchanged. If `input` has no lazy children, returns `input` as-is (no
/// reallocation). Null input is passed through.
///
/// Use at pipeline boundaries that consume values (UDF evaluation, writers,
/// result comparison). Operators that simply forward rows do NOT need to call
/// this — `LazyComplexVector` passes through like any other `VectorPtr`.
RowVectorPtr decodeLazyColumns(
    const RowVectorPtr& input,
    memory::MemoryPool* pool);

/// Selective overload: decodes only children at indices in `columns`.
/// Columns outside the set pass through unchanged (lazy stays lazy, regular
/// stays regular). Use this for Case-2 operators (FilterProject, Generator,
/// HashAggregation agg-args) that only need to materialize a subset of
/// complex columns. Returns `input` unchanged if nothing needs decoding.
RowVectorPtr decodeLazyColumns(
    const RowVectorPtr& input,
    memory::MemoryPool* pool,
    const std::unordered_set<column_index_t>& columns);

/// Per-column lazy dispatch applied by the Driver at the `addInput` seam.
/// For each position `i` in `modes`:
///   - `kAny`          : child passes through unchanged.
///   - `kForceDecoded` : if the child is `LazyComplexVector` it is decoded
///                       back to its original complex type.
///   - `kForceLazy`    : if the child is a complex type (ROW / ARRAY / MAP)
///                       and not yet `LazyComplexVector`, it is encoded.
/// `modes.size()` must equal `input->children().size()`, otherwise the
/// input is returned unchanged (no-op when operator declares no preference).
/// Returns the input unchanged when no columns needed transforming.
enum class InputLazyMode : uint8_t {
  kAny = 0,
  kForceDecoded = 1,
  kForceLazy = 2,
};
RowVectorPtr applyLazyInputModes(
    const RowVectorPtr& input,
    const std::vector<InputLazyMode>& modes,
    memory::MemoryPool* pool);

/// Convenience: returns a size-`size` InputLazyMode vector with `mode` set
/// at every index listed in `channels`, and `kAny` elsewhere. Channels
/// >= `size` are ignored. Used by operators that want to declare a
/// per-column policy for a sparse subset (e.g. FilterProject referenced
/// fields, Generator generate channels).
std::vector<InputLazyMode> makeInputLazyModes(
    size_t size,
    const std::vector<column_index_t>& channels,
    InputLazyMode mode);

/// Wire-schema helper for the bundled shuffle path: strips every complex
/// field (ROW / ARRAY / MAP) from `type` and appends a single VARBINARY
/// field named `__lazy_bundle__` iff any complex was present. The wire
/// carries one VARBINARY column holding every row's complex-column bytes
/// concatenated, independent of the original complex-column count.
/// Returns `type` unchanged when the codec is inactive or there are no
/// complex fields.
RowTypePtr lazyBundleWireRowType(const RowTypePtr& type);

/// Shuffle-writer side: packs every `LazyComplexVector` child of `input`
/// into one trailing VARBINARY child and returns a RowVector declared
/// with `lazyBundleWireRowType(input->type())`. Per-row layout of the
/// bundle column:
///
///   [ null-bitmap : ceil(N/8) bytes ]
///   for each non-null complex column j (in the original order):
///     [ len_j : uint32_t LE ][ bytes_j ]
///
/// Non-complex children pass through at their collapsed position.
/// Returns `input` unchanged when the codec is inactive or no child is
/// `LazyComplexVector`.
RowVectorPtr toLazyBundleWireRowVector(
    const RowVectorPtr& input,
    memory::MemoryPool* pool);

/// Shuffle-reader side: inverse of `toLazyBundleWireRowVector`. Splits
/// the trailing bundle VARBINARY child of `wire` back into one
/// `LazyComplexVector` per complex position of the plan-declared
/// `outputType`. Non-complex children pass through at their positions.
/// The reconstructed per-column `FlatVector<StringView>`s share the
/// bundle's `stringBuffers_` — zero byte copy. Returns `wire` unchanged
/// when the codec is inactive or `outputType` has no complex fields.
RowVectorPtr fromLazyBundleWireRowVector(
    const RowVectorPtr& wire,
    const RowTypePtr& outputType,
    memory::MemoryPool* pool);

/// Allocates a fresh child vector suitable for an operator's output `result`
/// at the given column `type` and `size`. When a lazy codec is active and
/// `type` is complex (`ROW`/`ARRAY`/`MAP`), returns a pre-allocated
/// `LazyComplexVector` so that `RowContainer::extractColumn` can write the
/// stored bytes into its inner `FlatVector<StringView>`. Otherwise returns
/// `BaseVector::create(type, size, pool)` — the existing behaviour.
VectorPtr allocateLazyAwareChild(
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool);

/// Allocates a RowVector where each complex child is lazy-aware per
/// `allocateLazyAwareChild`. Equivalent to `BaseVector::create(schema, size,
/// pool)` when no lazy codec is active. Use this in operator `getOutput` /
/// `prepareOutput` paths that produce complex-column-carrying output. A
/// cached `output_` containing LazyComplexVector children can be recycled
/// across batches via `BaseVector::prepareForReuse`; LAZY_COMPLEX is on the
/// reusable-encoding whitelist and `LazyComplexVector::prepareForReuse`
/// drops the prior batch's encoded-bytes arena.
RowVectorPtr allocateLazyAwareRowVector(
    const RowTypePtr& schema,
    vector_size_t size,
    memory::MemoryPool* pool);

/// Allocates a RowVector where the first `numLazyAwareCols` children use
/// `allocateLazyAwareChild` and the remaining children use plain
/// `BaseVector::create`. Useful for operators whose output layout is
/// `[input cols..., derived cols...]` and only the input-col prefix
/// should be lazy-aware (Window, TopNRowNumber row-number tail).
RowVectorPtr allocateLazyAwareRowVectorPrefix(
    const RowTypePtr& schema,
    vector_size_t size,
    size_t numLazyAwareCols,
    memory::MemoryPool* pool);

} // namespace bytedance::bolt

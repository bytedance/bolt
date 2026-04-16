/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/connectors/hive/PaimonRowIterator.h"
namespace bytedance::bolt::connector::hive {

struct PaimonEngine {
  /// Adds a single logical input row into the engine's internal merge state.
  ///
  /// Call this after `setResult()` has been called for the current output
  /// batch. Implementations may update internal state only, append finalized
  /// rows into `result`, or both, but they must not assume the newly added row
  /// is immediately safe to emit to the caller.
  ///
  /// Returns the current physical size of `result`. Callers should use
  /// `finalizeCompletedGroups()` to determine how many rows are actually safe
  /// to expose as output.
  virtual vector_size_t add(PaimonRowIteratorPtr iterator) = 0;

  /// Finalizes any groups that are known to be complete before `nextInput`
  /// would be consumed.
  ///
  /// `nextInput` is the next row that the reader intends to feed into `add()`,
  /// or `nullptr` when the input stream is exhausted. Engines should use this
  /// boundary signal to decide whether their current in-flight group can be
  /// safely materialized into `result` without exposing partially merged state
  /// across `next()` calls.
  ///
  /// Returns the number of rows in `result` that are complete and safe for the
  /// reader to return to its caller.
  virtual vector_size_t finalizeCompletedGroups(
      const PaimonRowIteratorPtr& nextInput) = 0;

  /// Flushes all remaining engine state at end-of-stream.
  ///
  /// Call this exactly once after no more input rows remain. This is equivalent
  /// to forcing a final boundary and must leave the engine with no pending
  /// groups from the current stream.
  ///
  /// Returns the final number of rows that are safe to expose from `result`.
  virtual vector_size_t finish() = 0;

  /// Installs the output row vector for the current reader batch.
  ///
  /// The reader must call this before `add()`, `finalizeCompletedGroups()`, or
  /// `finish()` for a batch. Engines may append finalized rows into this vector
  /// and may also keep in-progress state that references it, subject to each
  /// engine's own lifetime rules.
  virtual void setResult(RowVectorPtr result_) {
    result = result_;
  }

  /// Appends the row referenced by `iterator` into `output`.
  ///
  /// This helper is intended for engines whose finalized output row is exactly
  /// the input row selected by merge semantics.
  void append(VectorPtr output, PaimonRowIterator& iterator) {
    VLOG(2) << "Appending:"
            << "-->" << iterator.values->toString(iterator.rowIndex);
    vector_size_t targetIndex = output->size();
    output->resize(output->size() + 1);
    output->copy(iterator.values.get(), targetIndex, iterator.rowIndex, 1);
  }

  /// Removes the last appended row from `output`.
  ///
  /// This helper is for engines that model retract-like behavior by undoing the
  /// most recent append.
  void remove(VectorPtr output, PaimonRowIterator& iterator) {
    output->resize(output->size() - 1);
  }

 protected:
  RowVectorPtr result;
};

} // namespace bytedance::bolt::connector::hive

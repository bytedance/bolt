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

#pragma once

#include <folly/CPortability.h>
#include <folly/Synchronized.h>

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/core/PlanNode.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/AggregateUtil.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/InlineFlatten.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::core {
class ExpressionEvaluator;
}
namespace bytedance::bolt::exec {

class AggregateFunctionSignature;

// Returns true if aggregation receives raw (unprocessed) input, e.g. partial
// and single aggregation.
bool isRawInput(core::AggregationNode::Step step);

// Returns false if aggregation produces final result, e.g. final
// and single aggregation.
bool isPartialOutput(core::AggregationNode::Step step);

class Aggregate {
 protected:
  explicit Aggregate(TypePtr resultType) : resultType_(std::move(resultType)) {}

 public:
  virtual ~Aggregate() {}

  const TypePtr& resultType() const {
    return resultType_;
  }

  // Returns the fixed number of bytes the accumulator takes on a group
  // row. Variable width accumulators will reference the variable
  // width part of the state from the fixed part.
  virtual int32_t accumulatorFixedWidthSize() const = 0;

  /// Returns the alignment size of the accumulator.  Some types such as
  /// int128_t require aligned access.  This value must be a power of 2.
  virtual int32_t accumulatorAlignmentSize() const {
    return 1;
  }

  // Return true if accumulator is allocated from external memory, e.g. memory
  // not managed by Bolt.
  virtual bool accumulatorUsesExternalMemory() const {
    return false;
  }

  // Returns true if the accumulator never takes more than
  // accumulatorFixedWidthSize() bytes. If this is false, the
  // accumulator needs to track its changing variable length footprint
  // using RowSizeTracker (Aggregate::trackRowSize), see ArrayAggAggregate for
  // sample usage. A group row with at least one variable length key or
  // aggregate will have a 32-bit slot at offset RowContainer::rowSize_ for
  // keeping track of per-row size. The size is relevant for keeping caps on
  // result set and spilling batch sizes with skewed data.
  virtual bool isFixedSize() const {
    return true;
  }

  /// Returns true if toIntermediate() is supported.
  virtual bool supportsToIntermediate() const {
    return false;
  }

  void setAllocator(HashStringAllocator* allocator) {
    setAllocatorInternal(allocator);
    pool_ = allocator->pool();
  }

  /// Called for functions that take one or more lambda expression as input.
  /// These expressions must appear after all non-lambda inputs.
  /// These expressions cannot use captures.
  ///
  /// @param lambdaExpressions A list of lambda inputs (in the order they appear
  /// in function call).
  /// @param expressionEvaluator An instance of ExpressionEvaluator to use for
  /// evaluating lambda expressions.
  void setLambdaExpressions(
      std::vector<core::LambdaTypedExprPtr> lambdaExpressions,
      std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator);

  // Sets the offset and null indicator position of 'this'.
  // @param offset Offset in bytes from the start of the row of the accumulator
  // @param nullByte Offset in bytes from the start of the row of the null flag
  // @param nullMask The specific bit in the nullByte that stores the null flag
  // @param rowSizeOffset The offset of a uint32_t row size from the start of
  // the row. Only applies to accumulators that store variable size data out of
  // line. Fixed length accumulators do not use this. 0 if the row does not have
  // a size field.
  void setOffsets(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t rowSizeOffset) {
    setOffsetsInternal(offset, nullByte, nullMask, rowSizeOffset);
  }

  // Initializes null flags and accumulators for newly encountered groups.  This
  // function should be called only once for each group.
  //
  // @param groups Pointers to the start of the new group rows.
  // @param indices Indices into 'groups' of the new entries.
  virtual void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) = 0;

  // Single Aggregate instance is able to take both raw data and
  // intermediate result as input based on the assumption that Partial
  // accumulator and Final accumulator are of the same type.
  //
  // Updates partial accumulators from raw input data.
  // @param groups Pointers to the start of the group rows. These are aligned
  // with the 'args', e.g. data in the i-th row of the 'args' goes to the i-th
  // group. The groups may repeat if different rows go into the same group.
  // @param rows Rows of the 'args' to add to the accumulators. These may not be
  // contiguous if the aggregation has mask or is configured to drop null
  // grouping keys. The latter would be the case when aggregation is followed
  // by the join on the grouping keys. 'rows' is guaranteed to have at least one
  // active row.
  // @param args Raw input.
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates final accumulators from intermediate results.
  // @param groups Pointers to the start of the group rows. These are aligned
  // with the 'args', e.g. data in the i-th row of the 'args' goes to the i-th
  // group. The groups may repeat if different rows go into the same group.
  // @param rows Rows of the 'args' to add to the accumulators. These may not be
  // contiguous if the aggregation has mask or is configured to drop null
  // grouping keys. The latter would be the case when aggregation is followed
  // by the join on the grouping keys. 'rows' is guaranteed to have at least one
  // active row.
  // @param args Intermediate results produced by extractAccumulators().
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates the single partial accumulator from raw input data for global
  // aggregation.
  // @param group Pointer to the start of the group row.
  // @param rows Rows of the 'args' to add to the accumulators. These may not
  // be contiguous if the aggregation has mask. 'rows' is guaranteed to have at
  // least one active row.
  // @param args Raw input to add to the accumulators.
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Updates the single final accumulator from intermediate results for global
  // aggregation.
  // @param group Pointer to the start of the group row.
  // @param rows Rows of the 'args' to add to the accumulators. These may not
  // be contiguous if the aggregation has mask. 'rows' is guaranteed to have at
  // least one active row.
  // @param args Intermediate results produced by extractAccumulators().
  // @param mayPushdown True if aggregation can be pushdown down via LazyVector.
  // The pushdown can happen only if this flag is true and 'args' is a single
  // LazyVector.
  virtual void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) = 0;

  // Extracts final results (used for final and single aggregations).
  // @param groups Pointers to the start of the group rows.
  // @param numGroups Number of groups to extract results from.
  // @param result The result vector to store the results in.
  //
  // 'result' and its parts are expected to be singly referenced. If
  // other threads or operators hold references that they would use
  // after 'result' has been updated by this, effects will be unpredictable.
  // This method should not have side effects, i.e., calling this method
  // doesn't change the content of the accumulators. This is needed for an
  // optimization in Window operator where aggregations for expanding frames are
  // computed incrementally.
  virtual void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) = 0;

  // Extracts partial results (used for partial and intermediate aggregations).
  // @param groups Pointers to the start of the group rows.
  // @param numGroups Number of groups to extract results from.
  // @param result The result vector to store the results in.
  //
  // See comment on 'result' and side effects in extractValues().
  virtual void
  extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result) = 0;

  /// Produces an accumulator initialized from a single value for each
  /// row in 'rows'. The raw arguments of the aggregate are in 'args',
  /// which have the same meaning as in addRawInput. The result is
  /// placed in 'result'. 'result' is expected to be a writable flat vector of
  /// the right type.
  ///
  /// @param rows A set of rows to produce intermediate results for. The
  /// 'result' is expected to have rows.size() rows. Invalid rows represent rows
  /// that were masked out, these need to have correct intermediate results as
  /// well. It is possible that all entries in 'rows' are invalid (masked out).
  virtual void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const {
    BOLT_NYI("toIntermediate not supported");
  }

  /// does the accumulator type support serialize and deserialize
  /// memory after serialize
  ///   ------<accumulator>---- // fix part
  ///           --------------- // extra variable memory
  /// after serialize, the variable part of accumulator should serialize into
  /// extra variable memory. when deserilize, accumulator should build from fix
  /// part and extra variable memory, note: deserialized accumulator should not
  /// allocator extra memory except the variable memory with its serialized
  /// data.
  virtual bool supportAccumulatorSerde() const {
    return false;
  }

  /// return the extra buffer
  virtual uint32_t getAccumulatorSerializeSize(char* group) const {
    BOLT_NYI("getAccumulatorSerializeSize not supported");
  }

  // serialize the accumulator in group into 'dst' with memory size of 'size'
  virtual char* serializeAccumulator(char* group, char* dst) const {
    BOLT_NYI("serializeAccumulator not supported");
  }

  // deserialize the accumulator with data from src, warning: should not alloc
  // extra memory in deserialize
  virtual char* deserializeAccumulator(char* group, char* src) const {
    BOLT_NYI("deserializeAccumulator not supported");
  }

  // Frees any out of line storage for the accumulator in
  // 'groups'. No-op for fixed length accumulators.
  virtual void destroy(folly::Range<char**> /*groups*/) {}

  // Clears state between reuses, e.g. this is called before reusing
  // the aggregation operator's state after flushing a partial
  // aggregation.
  void clear() {
    clearInternal();
  }

  void enableValidateIntermediateInputs() {
    validateIntermediateInputs_ = true;
  }

  /// Creates an instance of aggregate function to accumulate a mix of raw input
  /// and intermediate results and produce either intermediate or final result.
  ///
  /// The caller will call setAllocator and setOffsets before starting to add
  /// data via initializeNewGroups, addRawInput, addIntermediateResults, etc.
  ///
  /// @param name Function name, e.g. min, max, sum, avg.
  /// @param step Either kPartial or kSingle. Determines the type of result:
  /// intermediate if kPartial, final if kSingle. Partial and intermediate
  /// aggregations create functions using kPartial. Single and final
  /// aggregations create functions using kSingle.
  /// @param argTypes Raw input types. Combined with the function name, uniquely
  /// identifies the function.
  /// @param resultType Intermediate result type if step is kPartial. Final
  /// result type is step is kFinal. This parameter is redundant since it can be
  /// derived from rawInput types and step. Present for legacy reasons.
  /// @param config Query config.
  /// @return An instance of the aggregate function.
  static std::unique_ptr<Aggregate> create(
      const std::string& name,
      core::AggregationNode::Step step,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& resultType,
      const core::QueryConfig& config);

  // Returns the intermediate type for 'name' with signature
  // 'argTypes'. Throws if cannot resolve.
  static TypePtr intermediateType(
      const std::string& name,
      const std::vector<TypePtr>& argTypes);

  virtual bool isSpillExtract() const {
    return isSpillExtract_;
  }

  virtual void setSpillExtract(bool spillExtract) {
    isSpillExtract_ = spillExtract;
  }

 protected:
  virtual void setAllocatorInternal(HashStringAllocator* allocator);

  virtual void setOffsetsInternal(
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t rowSizeOffset);

  virtual void clearInternal();

  // Shorthand for maintaining accumulator variable length size in
  // accumulator update methods. Use like: { auto tracker =
  // trackRowSize(group); update(group); }
  RowSizeTracker<char, uint32_t> trackRowSize(char* group) {
    BOLT_DCHECK(!isFixedSize());
    return RowSizeTracker<char, uint32_t>(group[rowSizeOffset_], *allocator_);
  }

  bool isNull(char* group) const {
    return numNulls_ && (group[nullByte_] & nullMask_);
  }

  // Sets null flag for all specified groups to true.
  // For any given group, this method can be called at most once.
  void setAllNulls(char** groups, folly::Range<const vector_size_t*> indices) {
    for (auto i : indices) {
      groups[i][nullByte_] |= nullMask_;
    }
    numNulls_ += indices.size();
  }

  inline bool setNull(char* group) {
    if (group[nullByte_] & nullMask_) {
      return false;
    }
    group[nullByte_] |= nullMask_;
    ++numNulls_;
    return true;
  }

  inline bool clearNull(char* group) {
    if (numNulls_) {
      uint8_t mask = group[nullByte_];
      if (mask & nullMask_) {
        group[nullByte_] = mask & ~nullMask_;
        --numNulls_;
        return true;
      }
    }
    return false;
  }

  template <typename T>
  FOLLY_ALWAYS_INLINE T* value(char* group) const {
    BOLT_DCHECK_EQ(
        reinterpret_cast<uintptr_t>(group + offset_) %
            accumulatorAlignmentSize(),
        0);
    return reinterpret_cast<T*>(group + offset_);
  }

  template <typename T>
  void destroyAccumulator(char* group) const {
    auto accumulator = value<T>(group);
    std::destroy_at(accumulator);
    memset(accumulator, 0, sizeof(T));
  }

  template <typename T>
  void destroyAccumulators(folly::Range<char**> groups) const {
    for (auto group : groups) {
      destroyAccumulator<T>(group);
    }
  }

  template <typename T>
  static uint64_t* getRawNulls(T* vector) {
    if (vector->mayHaveNulls()) {
      BufferPtr& nulls = vector->mutableNulls(vector->size());
      return nulls->asMutable<uint64_t>();
    } else {
      return nullptr;
    }
  }

  static void clearNull(uint64_t* rawNulls, vector_size_t index) {
    if (rawNulls) {
      bits::clearNull(rawNulls, index);
    }
  }

  const TypePtr resultType_;

  // Byte position of null flag in group row.
  int32_t nullByte_;
  uint8_t nullMask_;
  // Offset of fixed length accumulator state in group row.
  int32_t offset_;

  // Offset of uint32_t row byte size of row. 0 if there are no
  // variable width fields or accumulators on the row.  The size is
  // capped at 4G and will stay at 4G and not wrap around if growing
  // past this. This serves to track the batch size when extracting
  // rows. A size in excess of 4G would finish the batch in any case,
  // so larger values need not be represented.
  int32_t rowSizeOffset_ = 0;

  // Number of null accumulators in the current state of the aggregation
  // operator for this aggregate. If 0, clearing the null as part of update
  // is not needed.
  uint64_t numNulls_ = 0;
  HashStringAllocator* allocator_{nullptr};
  memory::MemoryPool* pool_{nullptr};
  std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator_{nullptr};
  std::vector<core::LambdaTypedExprPtr> lambdaExpressions_;

  // When selectivity vector has holes, in the pushdown, we need to generate a
  // different indices vector as the one we get from the DecodedVector is simply
  // sequential.
  std::vector<vector_size_t> pushdownCustomIndices_;

  bool validateIntermediateInputs_ = false;
  bool isSpillExtract_ = false;
};

using AggregateFunctionFactory = std::function<std::unique_ptr<Aggregate>(
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& resultType,
    const core::QueryConfig& config)>;

/// Register an aggregate function with the specified name and signatures. If
/// registerCompanionFunctions is true, also register companion aggregate and
/// scalar functions with it. When functions with `name` already exist, if
/// overwrite is true, existing registration will be replaced. Otherwise, return
/// false without overwriting the registry.
AggregateRegistrationResult registerAggregateFunction(
    const std::string& name,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>& signatures,
    const AggregateFunctionFactory& factory,
    bool registerCompanionFunctions = false,
    bool overwrite = false);

// Register an aggregation function with multiple names. Returns a vector of
// AggregateRegistrationResult, one for each name at the corresponding index.
std::vector<AggregateRegistrationResult> registerAggregateFunction(
    const std::vector<std::string>& names,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>& signatures,
    const AggregateFunctionFactory& factory,
    bool registerCompanionFunctions = false,
    bool overwrite = false);

/// Returns signatures of the aggregate function with the specified name.
/// Returns empty std::optional if function with that name is not found.
std::optional<std::vector<std::shared_ptr<AggregateFunctionSignature>>>
getAggregateFunctionSignatures(const std::string& name);

using AggregateFunctionSignatureMap =
    std::unordered_map<std::string, std::vector<AggregateFunctionSignaturePtr>>;

/// Returns a mapping of all Aggregate functions in registry.
/// The mapping is function name -> list of function signatures.
AggregateFunctionSignatureMap getAggregateFunctionSignatures();

struct AggregateFunctionEntry {
  std::vector<AggregateFunctionSignaturePtr> signatures;
  AggregateFunctionFactory factory;
};

using AggregateFunctionMap = folly::Synchronized<
    std::unordered_map<std::string, AggregateFunctionEntry>>;

AggregateFunctionMap& aggregateFunctions();

const AggregateFunctionEntry* FOLLY_NULLABLE
getAggregateFunctionEntry(const std::string& name);

struct CompanionSignatureEntry {
  std::string functionName;
  std::vector<FunctionSignaturePtr> signatures;
};

struct CompanionFunctionSignatureMap {
  std::vector<CompanionSignatureEntry> partial;
  std::vector<CompanionSignatureEntry> merge;
  std::vector<CompanionSignatureEntry> extract;
  std::vector<CompanionSignatureEntry> mergeExtract;
};

// Returns a map of potential companion function signatures the specified
// aggregation function would have. Notice that the registration of the
// specified aggregation function needs to register companion functions together
// for them to be used in queries.
std::optional<CompanionFunctionSignatureMap> getCompanionFunctionSignatures(
    const std::string& name);
} // namespace bytedance::bolt::exec

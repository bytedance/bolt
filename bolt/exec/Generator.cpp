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

#include "bolt/exec/Generator.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/Expr.h"
#include "bolt/functions/prestosql/json/JsonExtractor.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::exec {

Generator::Generator(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::GeneratorNode>& generatorNode,
    std::string operatorType)
    : Operator(
          driverCtx,
          generatorNode->outputType(),
          operatorId,
          generatorNode->id(),
          operatorType) {
  const auto& inputType = generatorNode->sources()[0]->outputType();
  const auto& generateFunc = generatorNode->generateFunction();
  const std::vector<core::TypedExprPtr>& genInputs = generateFunc->inputs();
  const auto& genField =
      std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(genInputs[0]);
  if (genField && genField->inputs().empty()) {
    generateChannels_.push_back(inputType->getChildIdx(genField->name()));
  } else {
    allExpr_.emplace_back(genInputs[0]);
  }
  genInput_.resize(1);
  generateDecoded_.resize(1);

  column_index_t outputChannel = 0;
  for (const auto& repCol : generatorNode->replicateCols()) {
    identityProjections_.emplace_back(
        inputType->getChildIdx(repCol->name()), outputChannel++);
  }
}

void Generator::initialize() {
  Operator::initialize();
  if (!allExpr_.empty()) {
    genExprs_ =
        std::make_unique<ExprSet>(std::move(allExpr_), operatorCtx_->execCtx());
  }
}

void Generator::preGen() {
  int32_t channel = 0;
  if (generateChannels_.size()) {
    genInput_[channel] = input_->childAt(generateChannels_[channel]);
  } else {
    if (genInput_[channel]) {
      genInput_[channel].reset();
    }
    auto size = input_->size();
    EvalCtx evalCtx(operatorCtx_->execCtx(), genExprs_.get(), input_.get());
    LocalSelectivityVector localRows(*operatorCtx_->execCtx(), size);
    auto* rows = localRows.get();
    rows->setAll();
    genExprs_->eval(0, 1, true, *rows, evalCtx, genInput_);
  }

  const auto& generateVector = genInput_[channel];
  generateDecoded_[channel].decode(*generateVector, inputRows_);
}

// explode
GeneratorExplode::GeneratorExplode(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::GeneratorNode>& generatorNode)
    : Generator(operatorId, driverCtx, generatorNode, "Generator explode"),
      withOrdinality_(generatorNode->withOrdinality()),
      isOuter_(generatorNode->isOuter()),
      outputBatchSize_(driverCtx->queryConfig().maxOutputBatchRows()) {
  input_ = nullptr;
  iterator_.reset();
}

RowVectorPtr GeneratorExplode::getOutput() {
  if (!input_) {
    return nullptr;
  }

  auto size = input_->size();
  if (iterator_.rowOffset_ == 0) {
    inputRows_.resize(size);
    // process genInput
    preGen();
  }

  // The max number of elements at each row across all generated columns.
  auto maxSizes = AlignedBuffer::allocate<int64_t>(size, pool(), 0);
  auto rawMaxSizes = maxSizes->asMutable<int64_t>();

  std::vector<const vector_size_t*> rawSizes(1);
  std::vector<const vector_size_t*> rawOffsets(1);

  int32_t channel = 0;
  const auto& generateVector = genInput_[channel];

  auto& currentDecoded = generateDecoded_[channel];
  auto currentIndices = currentDecoded.indices();

  const ArrayVector* generateBaseArray;
  const MapVector* generateBaseMap;
  if (generateVector->typeKind() == TypeKind::ARRAY) {
    generateBaseArray = currentDecoded.base()->as<ArrayVector>();
    rawSizes[channel] = generateBaseArray->rawSizes();
    rawOffsets[channel] = generateBaseArray->rawOffsets();
  } else {
    BOLT_CHECK(generateVector->typeKind() == TypeKind::MAP);
    generateBaseMap = currentDecoded.base()->as<MapVector>();
    rawSizes[channel] = generateBaseMap->rawSizes();
    rawOffsets[channel] = generateBaseMap->rawOffsets();
  }

  // Count max number of elements per row.
  auto currentSizes = rawSizes[channel];
  int numElements = 0;
  bool isIdentityMapping = true;
  BufferPtr nulls = nullptr;
  BufferPtr elementIndices = nullptr;

  if (isOuter_) {
    calRepeatedCount<true>(
        currentDecoded,
        size,
        currentSizes,
        currentIndices,
        rawMaxSizes,
        numElements);
  } else {
    calRepeatedCount<false>(
        currentDecoded,
        size,
        currentSizes,
        currentIndices,
        rawMaxSizes,
        numElements);
  }

  if (numElements == 0) {
    // All arrays/maps are null or empty.
    input_ = nullptr;
    iterator_.reset();
    return nullptr;
  }

  nulls = AlignedBuffer::allocate<bool>(numElements, pool(), bits::kNotNull);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  elementIndices = allocateIndices(numElements, pool());
  auto* rawElementIndices = elementIndices->asMutable<vector_size_t>();

  std::shared_ptr<FlatVector<int32_t>> ordinalityVector = nullptr;
  if (withOrdinality_) {
    ordinalityVector = std::dynamic_pointer_cast<FlatVector<int32_t>>(
        BaseVector::create(INTEGER(), numElements, pool()));
  }
  if (isOuter_) {
    setElementIndicesAndNulls<true>(
        currentDecoded,
        currentSizes,
        currentIndices,
        rawOffsets[channel],
        rawElementIndices,
        rawNulls,
        isIdentityMapping,
        ordinalityVector);
  } else {
    setElementIndicesAndNulls<false>(
        currentDecoded,
        currentSizes,
        currentIndices,
        rawOffsets[channel],
        rawElementIndices,
        rawNulls,
        isIdentityMapping,
        ordinalityVector);
  }

  // Create "indices" buffer to repeat rows as many times as there are elements
  // in the array (or map) in generateDecoded.
  auto repeatedIndices = allocateIndices(numElements, pool());
  auto* rawRepeatedIndices = repeatedIndices->asMutable<vector_size_t>();
  vector_size_t index = 0;
  for (auto row = iterator_.rowStart_; row < iterator_.rowOffset_; ++row) {
    for (auto i = 0; i < rawMaxSizes[row]; i++) {
      rawRepeatedIndices[index++] = row;
    }
  }

  // Wrap "replicated" columns in a dictionary using 'repeatedIndices'.
  std::vector<VectorPtr> outputs(outputType_->size());
  wrapIndirectChildren(
      identityProjections_,
      input_->children(),
      numElements,
      repeatedIndices,
      outputs);

  // Create generate columns.
  vector_size_t outputsIndex = identityProjections_.size();

  if (withOrdinality_) {
    // Set the ordinality at each result row to be the index of the element in
    // the original array (or map).
    auto rawOrdinality = ordinalityVector->mutableRawValues();
    for (auto row = iterator_.rowStart_; row < iterator_.rowOffset_; ++row) {
      auto maxSize = rawMaxSizes[row];
      std::iota(rawOrdinality, rawOrdinality + maxSize, 0);
      rawOrdinality += maxSize;
    }

    // Ordinality column is always in the beginning of generated columns
    outputs[outputsIndex++] = std::move(ordinalityVector);
  }

  if (isOuter_) {
    if (currentDecoded.base()->typeKind() == TypeKind::ARRAY) {
      // Generate column using Array elements
      auto generateBaseArray = currentDecoded.base()->as<ArrayVector>();
      outputs[outputsIndex++] = wrapChild(
          numElements, elementIndices, generateBaseArray->elements(), nulls);
    } else {
      // Generate two columns for Map keys and values vectors
      auto generateBaseMap = currentDecoded.base()->as<MapVector>();
      outputs[outputsIndex++] = wrapChild(
          numElements, elementIndices, generateBaseMap->mapKeys(), nulls);
      outputs[outputsIndex++] = wrapChild(
          numElements, elementIndices, generateBaseMap->mapValues(), nulls);
    }
  } else {
    if (currentDecoded.base()->typeKind() == TypeKind::ARRAY) {
      // Generate column using Array elements
      auto generateBaseArray = currentDecoded.base()->as<ArrayVector>();
      outputs[outputsIndex++] = isIdentityMapping
          ? generateBaseArray->elements()
          : wrapChild(
                numElements,
                elementIndices,
                generateBaseArray->elements(),
                nulls);
    } else {
      // Generate two columns for Map keys and values vectors
      auto generateBaseMap = currentDecoded.base()->as<MapVector>();
      outputs[outputsIndex++] = isIdentityMapping
          ? generateBaseMap->mapKeys()
          : wrapChild(
                numElements, elementIndices, generateBaseMap->mapKeys(), nulls);
      outputs[outputsIndex++] = isIdentityMapping
          ? generateBaseMap->mapValues()
          : wrapChild(
                numElements,
                elementIndices,
                generateBaseMap->mapValues(),
                nulls);
    }
  }

  if (iterator_.rowOffset_ == size) {
    input_ = nullptr;
    iterator_.reset();
  }
  return std::make_shared<RowVector>(
      pool(), outputType_, BufferPtr(nullptr), numElements, std::move(outputs));
}

// json_tuple
GeneratorJsonTuple::GeneratorJsonTuple(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::GeneratorNode>& generatorNode)
    : Generator(operatorId, driverCtx, generatorNode, "Generator json_tuple"),
      useSonicLibrary_(driverCtx->queryConfig().useSonicJson()) {
  const auto& inputType = generatorNode->sources()[0]->outputType();
  const auto& generateFunc = generatorNode->generateFunction();
  const std::vector<core::TypedExprPtr>& genInputs = generateFunc->inputs();

  numKeys_ = genInputs.size() - 1;
  jsonKeys_.resize(numKeys_);
  results_.resize(outputType_->size());
  jsonKeysIsNull_.resize(numKeys_, false);
  flatResults_.resize(outputType_->size(), nullptr);

  for (auto i = 0; i < numKeys_; ++i) {
    if (const auto& literal =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                genInputs[i + 1])) {
      if (literal->value().isNull()) { // key is null
        jsonKeysIsNull_[i] = true;
      } else {
        jsonKeys_[i] = literal->value().value<TypeKind::VARCHAR>();
        constKeyMap_.emplace_back(i);
      }
    } else if (
        const auto& field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                genInputs[i + 1])) {
      keyChannels_.emplace_back(inputType->getChildIdx(field->name()));
      colKeyMap_.emplace_back(i);
    } else {
      BOLT_FAIL("json keys should be constant string or direct column access");
    }
  }
}

void GeneratorJsonTuple::initializeResults(vector_size_t outputsIndex) {
  auto size = input_->size();
  for (int i = 0; i < numKeys_; ++i) {
    auto outputChannel = outputsIndex + i;
    if (jsonKeysIsNull_[i]) {
      results_[outputChannel] = BaseVector::createNullConstant(
          outputType_->childAt(outputChannel), size, pool());
    } else {
      results_[outputChannel] =
          BaseVector::create(outputType_->childAt(outputChannel), size, pool());
      flatResults_[outputChannel] =
          results_[outputChannel]->asFlatVector<StringView>();
      flatResults_[outputChannel]->mutableRawValues();
    }
  }
}

void GeneratorJsonTuple::decodedKeyColumn() {
  if (!keyChannels_.size())
    return;

  keyDecoded_.resize(keyChannels_.size());
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    const auto& keyInput = input_->childAt(keyChannels_[i]);
    keyDecoded_[i].decode(*keyInput, inputRows_);
  }
}

RowVectorPtr GeneratorJsonTuple::getOutput() {
  if (!input_) {
    return nullptr;
  }

  auto size = input_->size();
  inputRows_.resize(size);

  preGen();
  int32_t channel = 0;
  const auto& generateVector = genInput_[channel];
  auto& currentDecoded = generateDecoded_[channel];

  results_.resize(outputType_->size());

  for (const auto& projection : identityProjections_) {
    results_[projection.outputChannel] =
        input_->childAt(projection.inputChannel);
  }

  // create generate columns.
  vector_size_t keyOffset = identityProjections_.size();

  initializeResults(keyOffset);
  decodedKeyColumn();

  for (auto row = 0; row < size; ++row) {
    if (currentDecoded.isNullAt(row)) { // json str is null
      // constant keys
      for (auto i = 0; i < constKeyMap_.size(); ++i) {
        results_[constKeyMap_[i] + keyOffset]->setNull(row, true);
      }
      for (auto i = 0; i < colKeyMap_.size(); ++i) {
        results_[colKeyMap_[i] + keyOffset]->setNull(row, true);
      }
    } else {
      auto jsonStr = currentDecoded.valueAt<StringView>(row);

      std::vector<int32_t> offsets;
      std::vector<folly::Optional<folly::StringPiece>> paths;
      for (auto i = 0; i < constKeyMap_.size(); ++i) {
        paths.emplace_back(jsonKeys_[constKeyMap_[i]]);
        offsets.emplace_back(constKeyMap_[i]);
      }
      for (auto i = 0; i < colKeyMap_.size(); ++i) {
        offsets.emplace_back(colKeyMap_[i]);
        if (keyDecoded_[i].isNullAt(row)) {
          paths.emplace_back(folly::none);
        } else {
          auto key = keyDecoded_[i].valueAt<StringView>(row);
          paths.emplace_back(key.getString());
        }
      }
      auto vals = bytedance::bolt::functions::jsonExtractTuple(
          jsonStr, paths, false, useSonicLibrary_);
      for (size_t j = 0; j < offsets.size(); ++j) {
        auto& val = vals[j];
        val.hasValue()
            ? flatResults_[offsets[j] + keyOffset]->set(
                  row, StringView(val.value()))
            : flatResults_[offsets[j] + keyOffset]->setNull(row, true);
      }
    }
  }

  input_ = nullptr;
  return std::make_shared<RowVector>(
      pool(), outputType_, BufferPtr(nullptr), size, std::move(results_));
}

} // namespace bytedance::bolt::exec

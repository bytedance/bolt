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

#include <gtest/gtest.h>

#include "bolt/common/base/Fs.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/tests/ExpressionRunner.h"
#include "bolt/expression/tests/ExpressionVerifier.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/ExpressionsParser.h"
#include "bolt/parse/QueryPlanner.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/VectorSaver.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::test {

namespace {
/// Creates a RowVector from a list of child vectors. Uses _col0, _col1,..
/// auto-generated names for the RowType.
RowVectorPtr createRowVector(
    const std::vector<VectorPtr>& vectors,
    vector_size_t size,
    memory::MemoryPool* pool) {
  auto n = vectors.size();

  std::vector<std::string> names;
  names.reserve(n);
  std::vector<TypePtr> types;
  types.reserve(n);
  for (auto i = 0; i < n; ++i) {
    names.push_back(fmt::format("_col{}", i));
    types.push_back(vectors[i]->type());
  }

  return std::make_shared<RowVector>(
      pool, ROW(std::move(names), std::move(types)), nullptr, size, vectors);
}

RowVectorPtr evaluateAndPrintResults(
    exec::ExprSet& exprSet,
    const RowVectorPtr& data,
    const SelectivityVector& rows,
    core::ExecCtx& execCtx) {
  exec::EvalCtx evalCtx(&execCtx, &exprSet, data.get());

  std::vector<VectorPtr> results(1);
  exprSet.eval(rows, evalCtx, results);

  // Print the results.
  auto rowResult = createRowVector(results, rows.size(), execCtx.pool());
  std::cout << "Result: " << rowResult->type()->toString() << std::endl;
  exec::test::printResults(rowResult, std::cout);
  return rowResult;
}

vector_size_t adjustNumRows(vector_size_t numRows, vector_size_t size) {
  return numRows > 0 && numRows < size ? numRows : size;
}

void saveResults(
    const RowVectorPtr& results,
    const std::string& directoryPath) {
  auto path = common::generateTempFilePath(directoryPath.c_str(), "vector");
  BOLT_CHECK(
      path.has_value(),
      "Failed to create file for saving result vector in {} directory.",
      directoryPath);

  saveVectorToFile(results.get(), path.value().c_str());
  LOG(INFO) << "Saved the results in " << path.value() << ". "
            << results->size() << " rows: " << results->type()->toString();
}
} // namespace

std::vector<core::TypedExprPtr> ExpressionRunner::parseSql(
    const std::string& sql,
    const TypePtr& inputType,
    memory::MemoryPool* pool,
    const VectorPtr& complexConstants) {
  auto exprs = parse::parseMultipleExpressions(sql, {});

  std::vector<core::TypedExprPtr> typedExprs;
  typedExprs.reserve(exprs.size());
  for (const auto& expr : exprs) {
    typedExprs.push_back(
        core::Expressions::inferTypes(expr, inputType, pool, complexConstants));
  }
  return typedExprs;
}

void ExpressionRunner::run(
    const std::string& inputPath,
    const std::string& sql,
    const std::string& complexConstantsPath,
    const std::string& resultPath,
    const std::string& mode,
    vector_size_t numRows,
    const std::string& storeResultPath,
    const std::string& lazyColumnListPath,
    bool findMinimalSubExpression) {
  BOLT_CHECK(!sql.empty());

  auto queryCtx = core::QueryCtx::create();
  auto pool = memory::memoryManager()->addLeafPool();
  core::ExecCtx execCtx{pool.get(), queryCtx.get()};

  RowVectorPtr inputVector;
  if (inputPath.empty()) {
    inputVector = std::make_shared<RowVector>(
        pool.get(), ROW({}), nullptr, 1, std::vector<VectorPtr>{});
  } else {
    VectorPtr inputVectorPtr =
        restoreVectorFromFile(inputPath.c_str(), pool.get());
    inputVector = std::dynamic_pointer_cast<RowVector>(inputVectorPtr);
    BOLT_CHECK_NOT_NULL(
        inputVector,
        "Input vector is not a RowVector: {}",
        inputVectorPtr->toString());
    BOLT_CHECK_GT(inputVector->size(), 0, "Input vector must not be empty.");
  }

  std::vector<int> columnsToWrapInLazy;
  if (!lazyColumnListPath.empty()) {
    columnsToWrapInLazy =
        restoreStdVectorFromFile<int>(lazyColumnListPath.c_str());
  }

  parse::registerTypeResolver();

  if (mode == "query") {
    core::DuckDbQueryPlanner planner{pool.get()};

    if (inputVector->type()->size()) {
      LOG(INFO) << "Registering input vector as table t: "
                << inputVector->type()->toString();
      planner.registerTable("t", {inputVector});
    }

    auto plan = planner.plan(sql);
    auto results = exec::test::AssertQueryBuilder(plan).copyResults(pool.get());

    // Print the results.
    std::cout << "Result: " << results->type()->toString() << std::endl;
    exec::test::printResults(results, std::cout);

    if (!storeResultPath.empty()) {
      saveResults(results, storeResultPath);
    }
    return;
  }

  VectorPtr complexConstants{nullptr};
  if (!complexConstantsPath.empty()) {
    complexConstants =
        restoreVectorFromFile(complexConstantsPath.c_str(), pool.get());
  }
  auto typedExprs =
      parseSql(sql, inputVector->type(), pool.get(), complexConstants);

  VectorPtr resultVector;
  if (!resultPath.empty()) {
    resultVector = restoreVectorFromFile(resultPath.c_str(), pool.get());
  }

  SelectivityVector rows(adjustNumRows(numRows, inputVector->size()));

  LOG(INFO) << "Evaluating SQL expression(s): " << sql;

  if (mode == "verify") {
    auto verifier = test::ExpressionVerifier(&execCtx, {false, ""});
    try {
      verifier.verify(
          typedExprs,
          inputVector,
          std::move(resultVector),
          true,
          columnsToWrapInLazy);
    } catch (const std::exception& e) {
      if (findMinimalSubExpression) {
        VectorFuzzer::Options options;
        VectorFuzzer fuzzer(options, pool.get());
        computeMinimumSubExpression(
            std::move(verifier),
            fuzzer,
            typedExprs,
            inputVector,
            columnsToWrapInLazy);
      }
      throw;
    }

  } else if (mode == "common") {
    if (!columnsToWrapInLazy.empty()) {
      inputVector =
          VectorFuzzer::fuzzRowChildrenToLazy(inputVector, columnsToWrapInLazy);
    }
    exec::ExprSet exprSet(typedExprs, &execCtx);
    auto results = evaluateAndPrintResults(exprSet, inputVector, rows, execCtx);
    if (!storeResultPath.empty()) {
      saveResults(results, storeResultPath);
    }
  } else if (mode == "simplified") {
    exec::ExprSetSimplified exprSet(typedExprs, &execCtx);
    auto results = evaluateAndPrintResults(exprSet, inputVector, rows, execCtx);
    if (!storeResultPath.empty()) {
      saveResults(results, storeResultPath);
    }
  } else {
    BOLT_FAIL("Unknown expression runner mode: [{}].", mode);
  }
}

} // namespace bytedance::bolt::test

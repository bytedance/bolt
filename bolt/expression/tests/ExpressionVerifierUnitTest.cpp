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

#include <common/base/Exceptions.h>
#include <gtest/gtest.h>
#include <fstream>

#include "bolt/expression/tests/ExpressionVerifier.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/functions/Registerer.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/ExpressionsParser.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/type/Type.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::test {

namespace {

template <typename T>
struct AlwaysThrowsUserErrorFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    BOLT_USER_FAIL("expected");
  }
};

template <typename T>
struct AlwaysThrowsRuntimeErrorFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    BOLT_FAIL("expected");
  }
};

void removeDirecrtoryIfExist(
    std::shared_ptr<filesystems::FileSystem>& localFs,
    const std::string& folderPath) {
  if (localFs->exists(folderPath)) {
    localFs->rmdir(folderPath);
  }
  EXPECT_FALSE(localFs->exists(folderPath));
}

} // namespace

class ExpressionVerifierUnitTest : public testing::Test, public VectorTestBase {
 public:
  ExpressionVerifierUnitTest() {
    parse::registerTypeResolver();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, pool_.get());
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
};

TEST_F(ExpressionVerifierUnitTest, persistReproInfo) {
  filesystems::registerLocalFileSystem();
  auto reproFolder = bytedance::bolt::test::TempDirectoryPath::create();
  const auto reproPath = reproFolder->path;
  auto localFs = filesystems::getFileSystem(reproPath, nullptr);

  ExpressionVerifierOptions options{false, reproPath.c_str(), false};
  ExpressionVerifier verifier{&execCtx_, options};

  auto testReproPersistency = [this](
                                  ExpressionVerifier& verifier,
                                  const std::string& reproPath,
                                  auto& localFs,
                                  const bool forUserError,
                                  const bool forRuntimeError) {
    BOLT_USER_CHECK_NE(forUserError, forRuntimeError);

    auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
    auto plan = parseExpression("always_throws(c0)", asRowType(data->type()));

    removeDirecrtoryIfExist(localFs, reproPath);
    if (forUserError) {
      BOLT_ASSERT_THROW(verifier.verify({plan}, data, nullptr, false), "");
    } else {
      EXPECT_NO_THROW(verifier.verify({plan}, data, nullptr, false));
    }

    EXPECT_TRUE(localFs->exists(reproPath));
    EXPECT_FALSE(localFs->list(reproPath).empty());
    removeDirecrtoryIfExist(localFs, reproPath);
  };

  // User errors.
  {
    registerFunction<AlwaysThrowsUserErrorFunction, int32_t, int32_t>(
        {"always_throws"});
    testReproPersistency(verifier, reproPath, localFs, true, false);
  }

  // Runtime errors.
  {
    registerFunction<AlwaysThrowsRuntimeErrorFunction, int32_t, int32_t>(
        {"always_throws"});
    testReproPersistency(verifier, reproPath, localFs, false, true);
  }
}

} // namespace bytedance::bolt::test

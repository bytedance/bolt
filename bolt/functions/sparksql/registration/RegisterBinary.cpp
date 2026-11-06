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

#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/BinaryFunctions.h"
#include "bolt/functions/sparksql/EncodeDecode.h"
#include "bolt/functions/sparksql/Hash.h"
#include "bolt/functions/sparksql/MightContain.h"
#include "bolt/functions/sparksql/String.h"
namespace bytedance::bolt::functions::sparksql {

void registerBinaryFunctions(const std::string& prefix) {
  exec::registerStatefulVectorFunction(
      prefix + "hash", hashSignatures(), makeHash);
  exec::registerStatefulVectorFunction(
      prefix + "hash_with_seed", hashWithSeedSignatures(), makeHashWithSeed);
  exec::registerStatefulVectorFunction(
      prefix + "xxhash64", xxhash64Signatures(), makeXxHash64);
  exec::registerStatefulVectorFunction(
      prefix + "xxhash64_with_seed",
      xxhash64WithSeedSignatures(),
      makeXxHash64WithSeed);
  exec::registerStatefulVectorFunction(
      prefix + "hive_hash", hashSignatures(), makeHiveHash);

  registerFunction<Md5Function, Varchar, Varbinary>({prefix + "md5"});
  registerFunction<Md5Function, Varchar, Varchar>({prefix + "md5"});
  registerFunction<Sha1HexStringFunction, Varchar, Varbinary>(
      {prefix + "sha1"});
  registerFunction<Sha1HexStringFunction, Varchar, Varchar>({prefix + "sha1"});
  registerFunction<Sha2HexStringFunction, Varchar, Varbinary, int32_t>(
      {prefix + "sha2"});
  registerFunction<Sha2HexStringFunction, Varchar, Varchar, int32_t>(
      {prefix + "sha2"});
  // Register bloom filter function
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int64_t>(
      {prefix + "might_contain"});

  // Spark's encode/decode take a charset name, so they are stateful: a
  // constant charset argument lets the converter be built once per plan.
  exec::registerStatefulVectorFunction(
      prefix + "encode", encodeSignatures(), makeEncode);
  exec::registerStatefulVectorFunction(
      prefix + "decode", decodeSignatures(), makeDecode);
}

} // namespace bytedance::bolt::functions::sparksql

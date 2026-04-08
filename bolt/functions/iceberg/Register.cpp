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
 */

#include "bolt/functions/iceberg/Register.h"
#include "bolt/functions/iceberg/BucketFunction.h"
#include "bolt/functions/iceberg/DateTimeFunctions.h"
#include "bolt/functions/iceberg/Truncate.h"

namespace bytedance::bolt::functions::iceberg {

void registerFunctions(const std::string& prefix) {
  registerBucketFunctions(prefix);
  registerTruncateFunctions(prefix);
  registerDateTimeFunctions(prefix);
}

} // namespace bytedance::bolt::functions::iceberg

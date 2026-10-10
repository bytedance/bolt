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

#include <limits>

#include <memory>

#include "bolt/connectors/Connector.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/Split.h"

namespace bytedance::bolt {

exec::Split makeSplit(
    const std::string& connectorName,
    const std::string& filePath,
    dwio::common::FileFormat format,
    size_t offset = 0,
    size_t length = std::numeric_limits<size_t>::max());

} // namespace bytedance::bolt

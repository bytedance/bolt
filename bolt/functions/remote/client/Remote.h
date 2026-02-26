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

#include <folly/SocketAddress.h>
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/remote/if/gen-cpp2/RemoteFunction_types.h"
namespace bytedance::bolt::functions {

struct RemoteVectorFunctionMetadata : public exec::VectorFunctionMetadata {
  /// Network address of the servr to communicate with. Note that this can hold
  /// a network location (ip/port pair) or a unix domain socket path (see
  /// SocketAddress::makeFromPath()).
  folly::SocketAddress location;

  /// The serialization format to be used
  remote::PageFormat serdeFormat{remote::PageFormat::PRESTO_PAGE};
};

/// Registers a new remote function. It will use the metadata defined in
/// `RemoteVectorFunctionMetadata` to control the serialization format and
/// remote server address.
//
/// Remote functions are registered as regular statufull functions (using the
/// same internal catalog), and hence conflict if there already exists a
/// (non-remote) function registered with the same name. The `overwrite` flag
/// controls whether to overwrite in these cases.
void registerRemoteFunction(
    const std::string& name,
    std::vector<exec::FunctionSignaturePtr> signatures,
    const RemoteVectorFunctionMetadata& metadata = {},
    bool overwrite = true);

} // namespace bytedance::bolt::functions

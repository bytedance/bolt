/*
 * Copyright (c) International Business Machines Corporation
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

#include <initializer_list>
#include <string>
#include <utility>

#include <folly/dynamic.h>

namespace bytedance::bolt::connector {

/// Base class for options passed to ConnectorObjectFactory methods.
class ConnectorOptions {
 public:
  virtual ~ConnectorOptions() = default;
};

/// ConnectorOptions backed by a folly::dynamic object.
/// Connector implementations cast to this type to access options.
class DynamicConnectorOptions : public ConnectorOptions {
 public:
  folly::dynamic options;
};

/// Build a DynamicConnectorOptions from key-value pairs.
/// Usage: makeOptions({{"filterPushdownEnabled", true}, {"columnType", 1}})
inline DynamicConnectorOptions makeOptions(
    std::initializer_list<std::pair<std::string, folly::dynamic>> kvs) {
  DynamicConnectorOptions opts;
  opts.options = folly::dynamic::object;
  for (const auto& [k, v] : kvs) {
    opts.options[k] = v;
  }
  return opts;
}

} // namespace bytedance::bolt::connector

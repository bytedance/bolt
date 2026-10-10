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

#include "bolt/connectors/hive/storage_adapters/tos/TosCredentialsFile.h"

#include <boost/algorithm/string/trim.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <map>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::filesystems {
namespace {

constexpr const char* kAccessKey = "fs.tos.access-key-id";
constexpr const char* kSecretKey = "fs.tos.secret-access-key";
constexpr const char* kSessionToken = "fs.tos.session-token";
constexpr const char* kBucketPrefix = "fs.tos.bucket.";

std::string trim(std::string value) {
  boost::algorithm::trim(value);
  return value;
}

void parsePropertyNode(
    const boost::property_tree::ptree& property,
    std::map<std::string, std::string>& values) {
  auto name = property.get_optional<std::string>("name");
  auto value = property.get_optional<std::string>("value");

  if (name.has_value() && value.has_value()) {
    values[trim(name.value())] = trim(value.value());
  }
}

void collectPropertiesFromConfiguration(
    const boost::property_tree::ptree& configuration,
    std::map<std::string, std::string>& values) {
  for (const auto& entry : configuration) {
    if (entry.first == "property") {
      parsePropertyNode(entry.second, values);
    }
  }
}

std::optional<std::string> valueForKey(
    const std::map<std::string, std::string>& values,
    const std::string& key) {
  auto it = values.find(key);
  if (it == values.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::string bucketKey(std::string_view bucket, std::string_view suffix) {
  std::string key{kBucketPrefix};
  key.append(bucket.data(), bucket.size());
  key.append(".");
  key.append(suffix.data(), suffix.size());
  return key;
}

std::optional<std::string> bucketValue(
    const std::map<std::string, std::string>& values,
    std::string_view bucket,
    std::string_view bucketSuffix) {
  return valueForKey(values, bucketKey(bucket, bucketSuffix));
}

} // namespace

std::optional<TosCredentials> readTosCredentialsFile(
    const std::string& path,
    std::string_view bucket) {
  boost::property_tree::ptree tree;
  try {
    boost::property_tree::read_xml(path, tree);
  } catch (const boost::property_tree::xml_parser_error& e) {
    BOLT_FAIL("Unable to parse TOS credentials file {}: {}", path, e.what());
  }

  auto configuration = tree.get_child_optional("configuration");
  BOLT_CHECK(configuration.has_value(), "Empty TOS credentials file {}", path);

  std::map<std::string, std::string> values;
  collectPropertiesFromConfiguration(configuration.value(), values);

  TosCredentials credentials;
  credentials.bucketMatched =
      bucketValue(values, bucket, "access-key-id").has_value() ||
      bucketValue(values, bucket, "secret-access-key").has_value() ||
      bucketValue(values, bucket, "session-token").has_value();

  if (credentials.bucketMatched) {
    credentials.accessKey = bucketValue(values, bucket, "access-key-id");
    credentials.secretKey = bucketValue(values, bucket, "secret-access-key");
    credentials.sessionToken = bucketValue(values, bucket, "session-token");
  } else {
    credentials.accessKey = valueForKey(values, kAccessKey);
    credentials.secretKey = valueForKey(values, kSecretKey);
    credentials.sessionToken = valueForKey(values, kSessionToken);
  }

  if (!credentials.accessKey.has_value() &&
      !credentials.secretKey.has_value() &&
      !credentials.sessionToken.has_value()) {
    return std::nullopt;
  }

  if (credentials.bucketMatched) {
    BOLT_CHECK(
        credentials.hasAccessAndSecretKey(),
        "Invalid TOS credentials file {}: bucket '{}' credentials must include non-empty access-key-id and secret-access-key",
        path,
        bucket);
  } else {
    BOLT_CHECK(
        credentials.hasAccessAndSecretKey(),
        "Invalid TOS credentials file {}: base credentials must include non-empty fs.tos.access-key-id and fs.tos.secret-access-key",
        path);
  }
  return credentials;
}

} // namespace bytedance::bolt::filesystems

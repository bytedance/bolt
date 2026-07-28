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

#pragma once

#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include <folly/IPAddressV6.h>

#include "bolt/functions/Macros.h"

namespace bytedance::bolt::functions::flinksql {
namespace detail {

struct ParsedUrl {
  std::string protocol;
  std::string_view host;
  std::string_view path;
  std::optional<std::string_view> query;
  std::optional<std::string_view> ref;
  std::optional<std::string_view> authority;
  std::optional<std::string_view> userInfo;
};

FOLLY_ALWAYS_INLINE bool isProtocolFirstChar(char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

FOLLY_ALWAYS_INLINE bool isProtocolChar(char c) {
  return isProtocolFirstChar(c) || (c >= '0' && c <= '9') || c == '+' ||
      c == '-' || c == '.';
}

inline std::string_view trimUrl(std::string_view url) {
  while (!url.empty() && static_cast<unsigned char>(url.front()) <= ' ') {
    url.remove_prefix(1);
  }
  while (!url.empty() && static_cast<unsigned char>(url.back()) <= ' ') {
    url.remove_suffix(1);
  }
  return url;
}

inline bool isSupportedProtocol(std::string_view protocol) {
  return protocol == "file" || protocol == "ftp" || protocol == "http" ||
      protocol == "https" || protocol == "jar" || protocol == "mailto" ||
      protocol == "netdoc";
}

inline bool parsePort(std::string_view port) {
  if (port.empty()) {
    return true;
  }

  bool isNegative = false;
  if (port.front() == '+' || port.front() == '-') {
    isNegative = port.front() == '-';
    port.remove_prefix(1);
  }
  if (port.empty()) {
    return false;
  }

  int64_t value = 0;
  for (const auto c : port) {
    if (c < '0' || c > '9') {
      return false;
    }
    value = value * 10 + (c - '0');
    if (value > std::numeric_limits<int32_t>::max()) {
      return false;
    }
  }

  return !isNegative || value <= 1;
}

inline bool isValidIpv6Literal(std::string_view address) {
  const auto scopeStart = address.find('%');
  if (scopeStart != std::string_view::npos) {
    if (scopeStart + 1 == address.size()) {
      return false;
    }
    address = address.substr(0, scopeStart);
  }

  return folly::IPAddressV6::validate(
      folly::StringPiece(address.data(), address.size()));
}

inline bool parseAuthority(std::string_view authority, ParsedUrl& parsed) {
  parsed.authority = authority;
  parsed.host = std::string_view{};

  auto hostAndPort = authority;
  const auto userInfoEnd = authority.find('@');
  if (userInfoEnd != std::string_view::npos) {
    if (authority.find('@', userInfoEnd + 1) != std::string_view::npos) {
      return true;
    }
    parsed.userInfo = authority.substr(0, userInfoEnd);
    hostAndPort = authority.substr(userInfoEnd + 1);
  }

  if (hostAndPort.empty()) {
    return true;
  }

  if (hostAndPort.front() == '[') {
    const auto closingBracket = hostAndPort.find(']');
    if (closingBracket == std::string_view::npos) {
      return false;
    }
    if (!isValidIpv6Literal(hostAndPort.substr(1, closingBracket - 1))) {
      return false;
    }
    parsed.host = hostAndPort.substr(0, closingBracket + 1);
    const auto remainder = hostAndPort.substr(closingBracket + 1);
    if (remainder.empty()) {
      return true;
    }
    return remainder.front() == ':' && parsePort(remainder.substr(1));
  }

  const auto portStart = hostAndPort.find(':');
  if (portStart == std::string_view::npos) {
    parsed.host = hostAndPort;
    return true;
  }

  parsed.host = hostAndPort.substr(0, portStart);
  return parsePort(hostAndPort.substr(portStart + 1));
}

inline void splitRef(
    std::string_view& remainder,
    std::optional<std::string_view>& ref) {
  const auto refStart = remainder.find('#');
  if (refStart != std::string_view::npos) {
    ref = remainder.substr(refStart + 1);
    remainder = remainder.substr(0, refStart);
  }
}

inline void splitQuery(
    std::string_view& remainder,
    std::optional<std::string_view>& query,
    size_t start = 0) {
  const auto queryStart = remainder.find('?', start);
  if (queryStart != std::string_view::npos) {
    query = remainder.substr(queryStart + 1);
    remainder = remainder.substr(0, queryStart);
  }
}

inline bool parseUrl(std::string_view url, ParsedUrl& parsed);

inline bool parseJar(std::string_view remainder, ParsedUrl& parsed) {
  splitRef(remainder, parsed.ref);

  const auto separator = remainder.find("!/");
  if (separator == std::string_view::npos) {
    return false;
  }

  ParsedUrl nested;
  if (!parseUrl(remainder.substr(0, separator), nested)) {
    return false;
  }

  splitQuery(remainder, parsed.query, separator + 2);
  parsed.path = remainder;
  return true;
}

inline bool parseMailto(std::string_view remainder, ParsedUrl& parsed) {
  const auto refStart = remainder.find('#');
  if (refStart != std::string_view::npos) {
    remainder = remainder.substr(0, refStart);
  }
  if (remainder.empty()) {
    return false;
  }

  splitQuery(remainder, parsed.query);
  parsed.path = remainder;
  return true;
}

inline bool parseHierarchical(std::string_view remainder, ParsedUrl& parsed) {
  splitRef(remainder, parsed.ref);
  splitQuery(remainder, parsed.query);

  const bool isUncName =
      remainder.size() >= 4 && remainder.substr(0, 4) == "////";
  if (!isUncName && remainder.size() >= 2 && remainder[0] == '/' &&
      remainder[1] == '/') {
    remainder.remove_prefix(2);
    const auto pathStart = remainder.find('/');
    const auto authority = remainder.substr(0, pathStart);
    if (!parseAuthority(authority, parsed)) {
      return false;
    }
    parsed.path = pathStart == std::string_view::npos
        ? std::string_view{}
        : remainder.substr(pathStart);
    return true;
  }

  parsed.path = remainder;
  return true;
}

inline bool parseUrl(std::string_view url, ParsedUrl& parsed) {
  url = trimUrl(url);
  if (url.empty() || !isProtocolFirstChar(url.front())) {
    return false;
  }

  const auto protocolEnd = url.find(':');
  if (protocolEnd == std::string_view::npos) {
    return false;
  }
  for (size_t i = 1; i < protocolEnd; ++i) {
    if (!isProtocolChar(url[i])) {
      return false;
    }
  }

  parsed.protocol.reserve(protocolEnd);
  for (size_t i = 0; i < protocolEnd; ++i) {
    const auto c = url[i];
    parsed.protocol.push_back(
        c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c);
  }
  if (!isSupportedProtocol(parsed.protocol)) {
    return false;
  }

  const auto remainder = url.substr(protocolEnd + 1);
  if (parsed.protocol == "jar") {
    return parseJar(remainder, parsed);
  }
  if (parsed.protocol == "mailto") {
    return parseMailto(remainder, parsed);
  }
  return parseHierarchical(remainder, parsed);
}

} // namespace detail

template <typename T>
struct FlinkParseUrlFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url,
      const arg_type<Varchar>& partToExtract) {
    detail::ParsedUrl parsed;
    if (!detail::parseUrl(std::string_view(url.data(), url.size()), parsed)) {
      return false;
    }

    if (partToExtract == "PROTOCOL") {
      result = parsed.protocol;
      return true;
    }
    if (partToExtract == "HOST") {
      result = parsed.host;
      return true;
    }
    if (partToExtract == "PATH") {
      result = parsed.path;
      return true;
    }
    if (partToExtract == "QUERY") {
      if (!parsed.query.has_value()) {
        return false;
      }
      result = parsed.query.value();
      return true;
    }
    if (partToExtract == "REF") {
      if (!parsed.ref.has_value()) {
        return false;
      }
      result = parsed.ref.value();
      return true;
    }
    if (partToExtract == "FILE") {
      result = parsed.path;
      if (parsed.query.has_value()) {
        result += "?";
        result += parsed.query.value();
      }
      return true;
    }
    if (partToExtract == "AUTHORITY") {
      if (!parsed.authority.has_value()) {
        return false;
      }
      result = parsed.authority.value();
      return true;
    }
    if (partToExtract == "USERINFO") {
      if (!parsed.userInfo.has_value()) {
        return false;
      }
      result = parsed.userInfo.value();
      return true;
    }
    return false;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url,
      const arg_type<Varchar>& partToExtract,
      const arg_type<Varchar>& key) {
    if (partToExtract != "QUERY") {
      return false;
    }

    detail::ParsedUrl parsed;
    if (!detail::parseUrl(std::string_view(url.data(), url.size()), parsed) ||
        !parsed.query.has_value()) {
      return false;
    }

    const auto query = parsed.query.value();
    const auto keyView = std::string_view(key.data(), key.size());
    size_t start = 0;
    while (start <= query.size()) {
      const auto end = query.find('&', start);
      const auto parameter = query.substr(
          start,
          end == std::string_view::npos ? std::string_view::npos : end - start);
      const auto equals = parameter.find('=');
      if (equals != std::string_view::npos &&
          parameter.substr(0, equals) == keyView) {
        result = parameter.substr(equals + 1);
        return true;
      }
      if (end == std::string_view::npos) {
        break;
      }
      start = end + 1;
    }
    return false;
  }
};

} // namespace bytedance::bolt::functions::flinksql

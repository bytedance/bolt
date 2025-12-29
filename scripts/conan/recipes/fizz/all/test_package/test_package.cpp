/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include <fizz/client/FizzClientContext.h>
#include <fizz/client/PskCache.h>
#include <fizz/protocol/Types.h>
#include <fizz/record/Types.h>
#include <fizz/server/FizzServerContext.h>
#include <fizz/server/ReplayCache.h>
#include <folly/Optional.h>

#include <chrono>

int main() {
  fizz::client::FizzClientContext clientContext;
  clientContext.setSupportedVersions({fizz::ProtocolVersion::tls_1_3});
  clientContext.setSupportedCiphers(
      {fizz::CipherSuite::TLS_AES_128_GCM_SHA256});
  clientContext.setSupportedGroups({fizz::NamedGroup::x25519});
  clientContext.setSupportedSigSchemes(
      {fizz::SignatureScheme::ecdsa_secp256r1_sha256});
  clientContext.setSupportedPskModes({fizz::PskKeyExchangeMode::psk_dhe_ke});
  clientContext.setSupportedAlpns({"h2", "http/1.1"});
  clientContext.setSendEarlyData(true);
  clientContext.setCompatibilityMode(true);
  clientContext.setRequireAlpn(true);
  clientContext.setSendKeyShare(fizz::client::SendKeyShare::Always);
  clientContext.setMaxPskHandshakeLife(std::chrono::hours(1));

  auto pskCache = std::make_shared<fizz::client::BasicPskCache>();
  clientContext.setPskCache(pskCache);
  fizz::client::CachedPsk cachedPsk;
  cachedPsk.psk = "psk";
  cachedPsk.secret = "secret";
  cachedPsk.type = fizz::PskType::Resumption;
  cachedPsk.version = fizz::ProtocolVersion::tls_1_3;
  cachedPsk.cipher = fizz::CipherSuite::TLS_AES_128_GCM_SHA256;
  cachedPsk.group = fizz::NamedGroup::x25519;
  cachedPsk.maxEarlyDataSize = 1024;
  cachedPsk.alpn = std::string("h2");
  auto now = std::chrono::system_clock::now();
  cachedPsk.ticketAgeAdd = 1;
  cachedPsk.ticketIssueTime = now;
  cachedPsk.ticketExpirationTime = now + std::chrono::hours(1);
  cachedPsk.ticketHandshakeTime = now;
  clientContext.putPsk("identity", cachedPsk);

  fizz::server::FizzServerContext serverContext;
  serverContext.setSupportedVersions({fizz::ProtocolVersion::tls_1_3});
  serverContext.setSupportedCiphers(
      {{fizz::CipherSuite::TLS_AES_128_GCM_SHA256}});
  serverContext.setSupportedGroups({fizz::NamedGroup::x25519});
  serverContext.setSupportedSigSchemes(
      {fizz::SignatureScheme::ecdsa_secp256r1_sha256});
  serverContext.setSupportedPskModes({fizz::PskKeyExchangeMode::psk_dhe_ke});
  serverContext.setSupportedAlpns({"h2"});
  serverContext.setClientAuthMode(fizz::server::ClientAuthMode::Optional);
  serverContext.setVersionFallbackEnabled(true);
  serverContext.setMaxEarlyDataSize(2048);
  auto replayCache =
      std::make_shared<fizz::server::AllowAllReplayReplayCache>();
  fizz::server::ClockSkewTolerance skew{
      std::chrono::milliseconds(-1000), std::chrono::milliseconds(1000)};
  serverContext.setEarlyDataSettings(true, skew, replayCache);
  auto negotiated = serverContext.negotiateAlpn(
      {"h2", "http/1.1"}, folly::none);

  auto cached = clientContext.getPsk("identity");
  clientContext.removePsk("identity");

  bool ok = cached.hasValue();
  ok = ok && !clientContext.getPsk("identity").hasValue();
  ok = ok && negotiated && *negotiated == "h2";
  ok = ok && clientContext.getSendKeyShare() ==
      fizz::client::SendKeyShare::Always;
  ok = ok && serverContext.getAcceptEarlyData(fizz::ProtocolVersion::tls_1_3);
  return ok ? 0 : 1;
}

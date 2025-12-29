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

#include <folly/SocketAddress.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/SocketOptionMap.h>
#include <folly/portability/Sockets.h>
#include <wangle/acceptor/FizzConfig.h>
#include <wangle/acceptor/ServerSocketConfig.h>
#include <wangle/channel/Handler.h>
#include <wangle/channel/Pipeline.h>
#include <wangle/codec/LengthFieldBasedFrameDecoder.h>
#include <wangle/codec/LengthFieldPrepender.h>
#include <wangle/ssl/SSLContextConfig.h>

#include <memory>
#include <string>

namespace {

class CaptureHandler
    : public wangle::InboundHandler<std::unique_ptr<folly::IOBuf>> {
 public:
  void read(Context*, std::unique_ptr<folly::IOBuf> buf) override {
    if (buf) {
      bytes_ += buf->computeChainDataLength();
    }
  }

  size_t bytes() const {
    return bytes_;
  }

 private:
  size_t bytes_{0};
};

} // namespace

int main() {
  wangle::ServerSocketConfig serverConfig;
  serverConfig.acceptBacklog = 128;
  serverConfig.bindAddress = folly::SocketAddress("127.0.0.1", 0);
  serverConfig.reusePort = true;

  wangle::SSLContextConfig sslConfig;
  sslConfig.isLocalPrivateKey = false;
  sslConfig.alpnAllowMismatch = false;
  sslConfig.setCertificateBuf("cert", "key");
  sslConfig.clientCAFile = "ca.pem";
  serverConfig.sslContextConfigs.push_back(sslConfig);

  serverConfig.fizzConfig.supportedVersions = {
      fizz::ProtocolVersion::tls_1_3};
  serverConfig.fizzConfig.supportedCiphers = {
      {fizz::CipherSuite::TLS_AES_128_GCM_SHA256}};
  serverConfig.fizzConfig.supportedGroups = {fizz::NamedGroup::x25519};
  serverConfig.fizzConfig.supportedSigSchemes = {
      fizz::SignatureScheme::ecdsa_secp256r1_sha256};
  serverConfig.fizzConfig.supportedPskModes = {
      fizz::PskKeyExchangeMode::psk_dhe_ke};
  serverConfig.fizzConfig.acceptEarlyData = true;

  folly::SocketOptionMap socketOptions;
  socketOptions[folly::SocketOptionKey{
      SOL_SOCKET, SO_REUSEADDR, folly::SocketOptionKey::ApplyPos::PRE_BIND}] =
      1;
  serverConfig.setSocketOptions(socketOptions);

  using Pipeline =
      wangle::Pipeline<folly::IOBufQueue&, std::unique_ptr<folly::IOBuf>>;
  auto pipeline = Pipeline::create();
  auto decoder =
      std::make_shared<wangle::LengthFieldBasedFrameDecoder>(2, 1024, 0, 0, 2);
  auto capture = std::make_shared<CaptureHandler>();
  auto prepender = std::make_shared<wangle::LengthFieldPrepender>(2);
  pipeline->addBack(decoder);
  pipeline->addBack(capture);
  pipeline->addBack(prepender);
  pipeline->finalize();

  folly::IOBufQueue queue;
  std::string frame;
  frame.push_back('\0');
  frame.push_back('\5');
  frame += "hello";
  queue.append(folly::IOBuf::copyBuffer(frame));
  pipeline->read(queue);

  bool ok = serverConfig.isSSL() && serverConfig.hasExternalPrivateKey();
  ok = ok && pipeline->numHandlers() == 3;
  ok = ok && capture->bytes() == 5;
  return ok ? 0 : 1;
}

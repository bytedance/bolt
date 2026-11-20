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

#include <ClientConfig.h>
#include <arpa/inet.h>
#include <folly/File.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utils/crc64.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <limits>
#include <mutex>
#include <thread>
#include "bolt/common/config/Config.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/storage_adapters/tos/RegisterTosFileSystem.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosFileSystemExtension.h"
#include "bolt/dwio/common/FileSink.h"

namespace bytedance::bolt::filesystems {
namespace {

class LoopbackProxyExtension final : public TosFileSystemExtension {
 public:
  void customizeClientConfig(
      VolcengineTos::ClientConfig& config) const override {
    const std::string prefix = "http://tos-test-";
    if (config.endPoint.find(prefix) != 0) {
      return;
    }
    // The SDK rejects endpoints containing ports. Route a reserved .invalid
    // hostname through our loopback HTTP peer using its explicit proxy option.
    config.proxyHost = "127.0.0.1";
    config.proxyPort = std::stoi(config.endPoint.substr(prefix.size()));
    config.maxRetryCount = 0;
    config.connectionTimeout = 1000;
    config.requestTimeout = 2000;
  }
};

// A scripted HTTP peer keeps the real SDK, signing, streams and adapter in the
// test. It listens only on loopback and closes each connection after
// responding.
class TosHttpServer {
 public:
  explicit TosHttpServer(std::vector<std::string> responses)
      : socket_(::socket(AF_INET, SOCK_STREAM, 0), true),
        responses_(std::move(responses)) {
    static std::once_flag registered;
    std::call_once(registered, [] {
      BOLT_CHECK(registerTosFileSystemExtension(
          std::make_shared<LoopbackProxyExtension>()));
    });
    BOLT_CHECK_GE(socket_.fd(), 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    BOLT_CHECK_EQ(
        ::bind(
            socket_.fd(),
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)),
        0);
    socklen_t length = sizeof(address);
    BOLT_CHECK_EQ(
        ::getsockname(
            socket_.fd(), reinterpret_cast<sockaddr*>(&address), &length),
        0);
    endpoint_ = "http://tos-test-" + std::to_string(ntohs(address.sin_port)) +
        ".invalid";
    BOLT_CHECK_EQ(::listen(socket_.fd(), 16), 0);
    thread_ = std::thread([this] { serve(); });
  }

  ~TosHttpServer() {
    stopped_ = true;
    thread_.join();
  }

  std::shared_ptr<FileSystem> fileSystem() const {
    registerTosFileSystem();
    auto config = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{
            {"tos.endpoint", endpoint_},
            {"tos.region", "test-region"},
            {"tos.access.key", "test-access"},
            {"tos.secret.key", "test-secret"}});
    return getFileSystem(kPath, config);
  }

  const std::string& endpoint() const {
    return endpoint_;
  }

  std::vector<std::string> requests() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return requests_;
  }

  static std::string
  response(int status, std::string headers = {}, std::string body = {}) {
    if (headers.find("Content-Length:") == std::string::npos) {
      headers += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    }
    return "HTTP/1.1 " + std::to_string(status) + " Test\r\n" + headers +
        "Connection: close\r\n\r\n" + body;
  }

  static constexpr std::string_view kPath = "tos://test-bucket/object";

 private:
  void serve() {
    while (!stopped_) {
      pollfd descriptor{socket_.fd(), POLLIN, 0};
      if (::poll(&descriptor, 1, 50) <= 0) {
        continue;
      }
      folly::File connection(::accept(socket_.fd(), nullptr, nullptr), true);
      if (connection.fd() < 0) {
        return;
      }
      timeval timeout{2, 0};
      ::setsockopt(
          connection.fd(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
      ::setsockopt(
          connection.fd(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
      std::string request;
      size_t expected = std::string::npos;
      char buffer[4096];
      while (!stopped_ && request.size() < expected) {
        const auto count = ::recv(connection.fd(), buffer, sizeof(buffer), 0);
        if (count <= 0) {
          break;
        }
        request.append(buffer, count);
        const auto end = request.find("\r\n\r\n");
        if (expected == std::string::npos && end != std::string::npos) {
          expected = end + 4;
          std::string lower = request.substr(0, end);
          std::transform(
              lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
                return std::tolower(c);
              });
          const auto contentLength = lower.find("content-length:");
          if (contentLength != std::string::npos) {
            expected += std::stoull(lower.substr(contentLength + 15));
          }
          if (lower.find("expect: 100-continue") != std::string::npos) {
            const std::string interim = "HTTP/1.1 100 Continue\r\n\r\n";
            ::send(
                connection.fd(), interim.data(), interim.size(), MSG_NOSIGNAL);
          }
        }
      }
      std::string reply;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto index = requests_.size();
        requests_.push_back(std::move(request));
        reply = index < responses_.size() ? responses_[index] : response(400);
      }
      size_t sent = 0;
      while (sent < reply.size()) {
        const auto count = ::send(
            connection.fd(),
            reply.data() + sent,
            reply.size() - sent,
            MSG_NOSIGNAL);
        if (count <= 0) {
          break;
        }
        sent += count;
      }
    }
  }

  folly::File socket_;
  std::string endpoint_;
  const std::vector<std::string> responses_;
  mutable std::mutex mutex_;
  std::vector<std::string> requests_;
  std::atomic<bool> stopped_{false};
  std::thread thread_;
};

using Server = TosHttpServer;

TEST(TosReadWriteTest, readsRangesAndGaps) {
  Server server(
      {Server::response(200, "Content-Length: 10\r\n"),
       Server::response(206, {}, "234"),
       Server::response(206, {}, "123456")});
  auto fs = server.fileSystem();
  auto file = fs->openFileForRead(Server::kPath);
  EXPECT_EQ(file->size(), 10);
  EXPECT_EQ(file->pread(2, 3), "234");
  char first[2];
  char last[2];
  EXPECT_EQ(file->preadv(1, {{first, 2}, {nullptr, 2}, {last, 2}}), 6);
  EXPECT_EQ(std::string(first, 2), "12");
  EXPECT_EQ(std::string(last, 2), "56");
  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 3);
  EXPECT_NE(requests[1].find("bytes=2-4"), std::string::npos);
  EXPECT_NE(requests[2].find("bytes=1-6"), std::string::npos);
}

TEST(TosReadWriteTest, validatesRangesWithoutNetworkRequests) {
  Server server({Server::response(200, "Content-Length: 10\r\n")});
  auto file = server.fileSystem()->openFileForRead(Server::kPath);
  EXPECT_EQ(file->pread(0, 0), "");
  EXPECT_EQ(file->pread(10, 0), "");
  EXPECT_EQ(file->preadv(10, {}), 0);
  EXPECT_ANY_THROW(file->pread(11, 0));
  EXPECT_ANY_THROW(file->pread(std::numeric_limits<uint64_t>::max(), 2));
  EXPECT_ANY_THROW(file->pread(9, 2));
  EXPECT_EQ(server.requests().size(), 1);
}

TEST(TosReadWriteTest, rejectsShortReads) {
  Server server(
      {Server::response(200, "Content-Length: 10\r\n"),
       Server::response(206, {}, "12")});
  auto file = server.fileSystem()->openFileForRead(Server::kPath);
  EXPECT_ANY_THROW(file->pread(0, 3));
}

TEST(TosReadWriteTest, appendsToFlatAndHierarchicalBuckets) {
  std::string contents = "abcdef";
  const auto crcHeader = [&](size_t size) {
    return "x-tos-hash-crc64ecma: " +
        std::to_string(
               VolcengineTos::CRC64::CalcCRC(0, contents.data(), size)) +
        "\r\n";
  };
  for (const std::string type : {"fns", "hns"}) {
    const auto bucketResponse =
        Server::response(200, "x-tos-bucket-type: " + type + "\r\n");
    std::vector<std::string> responses{bucketResponse, Server::response(404)};
    if (type == "fns") {
      responses.push_back(bucketResponse);
    }
    responses.push_back(Server::response(
        200, "x-tos-next-append-offset: 3\r\n" + crcHeader(3)));
    if (type == "hns") {
      responses.push_back(bucketResponse);
    }
    responses.push_back(Server::response(
        200,
        std::string(
            type == "fns" ? "x-tos-next-append-offset: 6\r\n"
                          : "x-tos-next-modify-offset: 6\r\n") +
            crcHeader(6)));
    Server server(std::move(responses));
    auto file = server.fileSystem()->openFileForWrite(Server::kPath);
    file->append("abc");
    file->append("def");
    EXPECT_EQ(file->size(), 6);
    file->close();
    file->close();
    EXPECT_ANY_THROW(file->append("g"));
    auto requests = server.requests();
    ASSERT_EQ(requests.size(), 5);
    const auto firstWrite = type == "fns" ? 3 : 2;
    EXPECT_EQ(
        requests[firstWrite].substr(0, type == "fns" ? 4 : 3),
        type == "fns" ? "POST" : "PUT");
    EXPECT_EQ(
        requests[firstWrite].substr(requests[firstWrite].size() - 3), "abc");
    EXPECT_NE(requests[4].find("offset=3"), std::string::npos);
    EXPECT_NE(
        requests[4].find(type == "fns" ? "append=" : "modify="),
        std::string::npos);
    EXPECT_EQ(requests[4].substr(requests[4].size() - 3), "def");
  }
}

TEST(TosReadWriteTest, closesEmptyFile) {
  Server server(
      {Server::response(200, "x-tos-bucket-type: fns\r\n"),
       Server::response(404),
       Server::response(200)});
  auto file = server.fileSystem()->openFileForWrite(Server::kPath);
  file->append("");
  file->close();
  file->close();
  EXPECT_EQ(file->size(), 0);
  auto requests = server.requests();
  ASSERT_EQ(requests.size(), 3);
  EXPECT_EQ(requests[2].substr(0, 3), "PUT");
}

TEST(TosReadWriteTest, rejectsExistingOrInaccessibleObjects) {
  for (const int status : {200, 403}) {
    Server server(
        {Server::response(200, "x-tos-bucket-type: fns\r\n"),
         Server::response(status)});
    auto fs = server.fileSystem();
    EXPECT_ANY_THROW(fs->openFileForWrite(Server::kPath));
    EXPECT_EQ(server.requests().size(), 2);
  }
}

TEST(TosReadWriteTest, overwritesFlatObjectFromOffsetZero) {
  Server server(
      {Server::response(200, "x-tos-bucket-type: fns\r\n"),
       Server::response(200),
       Server::response(204),
       Server::response(200, "x-tos-bucket-type: fns\r\n"),
       Server::response(200, "x-tos-next-append-offset: 3\r\n")});
  FileOptions options;
  options.shouldThrowOnFileAlreadyExists = false;
  auto file = server.fileSystem()->openFileForWrite(Server::kPath, options);
  file->append("new");
  file->close();
  auto requests = server.requests();
  ASSERT_EQ(requests.size(), 5);
  EXPECT_EQ(requests[2].substr(0, 6), "DELETE");
  EXPECT_NE(requests[4].find("offset=0"), std::string::npos);
}

TEST(TosReadWriteTest, bucketCreationRequiresExplicitOption) {
  Server disabled({Server::response(404)});
  EXPECT_ANY_THROW(disabled.fileSystem()->openFileForWrite(Server::kPath));
  EXPECT_EQ(disabled.requests().size(), 1);
  Server enabled(
      {Server::response(404),
       Server::response(200),
       Server::response(404),
       Server::response(200)});
  FileOptions options;
  options.shouldCreateParentDirectories = true;
  auto file = enabled.fileSystem()->openFileForWrite(Server::kPath, options);
  file->close();
  auto requests = enabled.requests();
  ASSERT_EQ(requests.size(), 4);
  EXPECT_EQ(requests[1].substr(0, 3), "PUT");
}

TEST(TosReadWriteTest, overwritesHierarchicalObjectWithPut) {
  Server server(
      {Server::response(200, "x-tos-bucket-type: hns\r\n"),
       Server::response(200)});
  FileOptions options;
  options.shouldThrowOnFileAlreadyExists = false;
  auto file = server.fileSystem()->openFileForWrite(Server::kPath, options);
  file->append("replacement");
  file->close();
  auto requests = server.requests();
  ASSERT_EQ(requests.size(), 2);
  EXPECT_EQ(requests[1].substr(0, 3), "PUT");
  EXPECT_EQ(file->size(), 11);
}

TEST(TosReadWriteTest, permissionFailureNeverCreatesBucket) {
  Server server({Server::response(403)});
  FileOptions options;
  options.shouldCreateParentDirectories = true;
  EXPECT_ANY_THROW(
      server.fileSystem()->openFileForWrite(Server::kPath, options));
  EXPECT_EQ(server.requests().size(), 1);
}

TEST(TosReadWriteTest, fileSinkForwardsFileOptions) {
  Server server(
      {Server::response(404),
       Server::response(200),
       Server::response(404),
       Server::response(200)});
  registerTosFileSystem();
  FileOptions fileOptions;
  fileOptions.shouldCreateParentDirectories = true;
  fileOptions.values = {
      {"tos.endpoint", server.endpoint()},
      {"tos.region", "test-region"},
      {"tos.access.key", "test-access"},
      {"tos.secret.key", "test-secret"}};
  dwio::common::FileSink::Options options;
  options.fileOptions = &fileOptions;
  auto sink =
      dwio::common::FileSink::create(std::string(Server::kPath), options);
  sink->close();
  auto requests = server.requests();
  ASSERT_EQ(requests.size(), 4);
  EXPECT_EQ(requests[1].substr(0, 3), "PUT");
}

} // namespace
} // namespace bytedance::bolt::filesystems

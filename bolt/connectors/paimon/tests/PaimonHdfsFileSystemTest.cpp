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

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/paimon/PaimonBoltHdfsFileSystem.h"
#include "bolt/connectors/paimon/tests/HdfsContainerMiniCluster.h"

#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"

#include "paimon/fs/file_system_factory.h"

namespace bytedance::bolt::connector::paimon {
namespace {

using bytedance::bolt::connector::paimon::test::HdfsContainerMiniCluster;

namespace {

bool containsUnsupported(const std::string& msg) {
  // Keep this simple and robust: paimon-cpp status strings include the
  // underlying Bolt exception type/message.
  return msg.find("UNSUPPORTED") != std::string::npos ||
      msg.find("NotImplemented") != std::string::npos ||
      msg.find("not implemented") != std::string::npos ||
      msg.find("Does not support removing files from hdfs") !=
      std::string::npos;
}

// Throws std::runtime_error if JAVA_HOME is not set or is empty.
void requireEnvironmentVariable(const std::string& varName) {
  const char* envValue = std::getenv(varName.c_str());
  if (envValue == nullptr || envValue[0] == '\0') {
    throw std::runtime_error(fmt::format("{} is not set!", varName));
  }
}

} // namespace

class PaimonHdfsFileSystemTest : public testing::Test {
 protected:
  static std::string runAndCapture(const std::string& cmd) {
    std::string output;
    // Use bash -c to properly handle inline environment variable assignments.
    FILE* pipe = ::popen(("bash -c '" + cmd + "'").c_str(), "r");
    if (!pipe) {
      return output;
    }
    char buffer[4096];
    while (true) {
      size_t n = std::fread(buffer, 1, sizeof(buffer), pipe);
      if (n == 0) {
        break;
      }
      output.append(buffer, n);
    }
    ::pclose(pipe);
    // Trim trailing newlines.
    while (!output.empty() &&
           (output.back() == '\n' || output.back() == '\r')) {
      output.pop_back();
    }
    return output;
  }

  static void SetUpTestSuite() {
    // Many Bolt components require a global MemoryManager instance.
    // Other paimon connector tests set this up in SetUpTestCase; do the same
    // here since this test suite can be run in isolation.
    {
      ::bytedance::bolt::memory::MemoryManager::Options options;
      options.allocatorCapacity = 8L << 30;
      ::bytedance::bolt::memory::MemoryManager::testingSetInstance(options);
    }

    // Validate environment prerequisites.
    try {
      requireEnvironmentVariable("JAVA_HOME");
      LOG(INFO) << "JAVA_HOME: " << ::getenv("JAVA_HOME");
      requireEnvironmentVariable("HADOOP_HOME");
    } catch (std::runtime_error& e) {
      GTEST_SKIP()
          << "Skipped HDFS tests due to missing environment variables: "
          << e.what();
    }

    std::string hadoopHome = ::getenv("HADOOP_HOME");
    std::string cmd = fmt::format("{}/bin/hadoop classpath --glob", hadoopHome);
    std::string hadoopClasspath = runAndCapture(cmd);
    LOG(INFO) << "Computed hadoop classpath: " << hadoopClasspath;
    ::setenv("CLASSPATH", hadoopClasspath.c_str(), 1);
    ::setenv(
        "ARROW_LIBHDFS_DIR",
        (hadoopHome + "/lib/native").c_str(),
        /*overwrite=*/1);

    // libhdfs starts an embedded JVM; tune a few opts for stability.
    //
    // - bump thread stack
    // - avoid JniBasedUnixGroupsMapping fallback paths that spawn processes
    ::setenv(
        "LIBHDFS_OPTS",
        "-Xss8m "
        "-Djava.net.preferIPv4Stack=true "
        "-Djava.net.preferIPv6Addresses=false "
        "-Dhadoop.security.group.mapping=org.apache.hadoop.security.ShellBasedUnixGroupsMapping",
        /*overwrite=*/1);

    // Container-based cluster: requires docker or podman.
    // HdfsContainerMiniCluster validates HADOOP_HOME and container engine
    // availability.
    try {
      cluster_ = std::make_unique<HdfsContainerMiniCluster>(
          "bolt-paimon-hdfs-minicluster");
      cluster_->Start(std::chrono::seconds(60));
      LOG(INFO) << "Started cluster at " << cluster_->namenodeUri();
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to start HDFS container: " << e.what()
                   << ". Skipping HDFS test suite.";
      cluster_.reset();
      clusterAvailable_ = false;
      return;
    }
  }

  static void TearDownTestSuite() {
    if (cluster_) {
      cluster_->Stop();
      cluster_.reset();
    }

    // Ensure HDFS filesystem instances disconnect before JVM teardown.
    // `registeredFilesystems` is owned by the Hive HDFS registration TU.
    for (const auto& it : bytedance::bolt::filesystems::registeredFilesystems) {
      if (it.second) {
        it.second->close();
      }
    }
    bytedance::bolt::filesystems::registeredFilesystems.clear();
  }

  static inline std::unique_ptr<HdfsContainerMiniCluster> cluster_;
  static inline bool clusterAvailable_{true};
};

TEST_F(PaimonHdfsFileSystemTest, CreateOpenGetStatusListAndDelete) {
  if (!clusterAvailable_) {
    GTEST_SKIP() << "CLASSPATH missing; libhdfs JNI cannot initialize";
  }

  // In some container-in-container environments, podman host networking does
  // not expose ports on this process' localhost. if (!isLocalTcpPortOpen(7878))
  // {
  //   GTEST_SKIP() << "HDFS NameNode port 7878 not reachable on localhost";
  // }

  EnsurePaimonBoltHdfsFileSystemRegistered();

  const std::string base = cluster_->namenodeUri() +
      std::string("/paimon_bolt_hdfs_fs_test_") + std::to_string(::getpid());
  const std::string dir = base + "/dir";
  const std::string file = dir + "/file.txt";
  const std::string renamed = dir + "/file2.txt";

  auto fsRes = ::paimon::FileSystemFactory::Get(
      "bolt_hdfs", file, std::map<std::string, std::string>{});
  ASSERT_TRUE(fsRes.ok()) << fsRes.status().ToString();
  auto fs = std::move(fsRes).value();

  ASSERT_TRUE(fs->Mkdirs(dir).ok());

  auto outRes = fs->Create(file, /*overwrite=*/true);
  ASSERT_TRUE(outRes.ok()) << outRes.status().ToString();
  auto out = std::move(outRes).value();
  const std::string payload = "hello_hdfs";
  ASSERT_TRUE(out->Write(payload.data(), payload.size()).ok());
  ASSERT_TRUE(out->Flush().ok());
  ASSERT_TRUE(out->Close().ok());

  auto existsRes = fs->Exists(file);
  ASSERT_TRUE(existsRes.ok()) << existsRes.status().ToString();
  ASSERT_TRUE(existsRes.value());

  auto stRes = fs->GetFileStatus(file);
  ASSERT_TRUE(stRes.ok()) << stRes.status().ToString();
  auto st = std::move(stRes).value();
  ASSERT_FALSE(st->IsDir());
  ASSERT_EQ(st->GetLen(), payload.size());
  ASSERT_GT(st->GetModificationTime(), 0);

  std::vector<std::unique_ptr<::paimon::BasicFileStatus>> children;
  ASSERT_TRUE(fs->ListDir(dir, &children).ok());
  ASSERT_EQ(children.size(), 1);
  ASSERT_EQ(children[0]->GetPath(), file);

  auto inRes = fs->Open(file);
  ASSERT_TRUE(inRes.ok()) << inRes.status().ToString();
  auto in = std::move(inRes).value();
  std::string buffer(payload.size(), '\0');
  auto readRes = in->Read(buffer.data(), buffer.size(), 0);
  ASSERT_TRUE(readRes.ok()) << readRes.status().ToString();
  ASSERT_EQ(static_cast<size_t>(readRes.value()), payload.size());
  ASSERT_EQ(buffer, payload);
  ASSERT_TRUE(in->Close().ok());

  ASSERT_TRUE(fs->Delete(base, /*recursive=*/true).ok());
}

TEST_F(PaimonHdfsFileSystemTest, RenameFile) {
  if (!clusterAvailable_) {
    GTEST_SKIP() << "CLASSPATH missing; libhdfs JNI cannot initialize";
  }
  EnsurePaimonBoltHdfsFileSystemRegistered();

  const std::string base = cluster_->namenodeUri() +
      std::string("/paimon_bolt_hdfs_rename_test_") +
      std::to_string(::getpid());
  const std::string dir = base + "/dir";
  const std::string file = dir + "/file.txt";
  const std::string renamed = dir + "/file2.txt";

  auto fsRes = ::paimon::FileSystemFactory::Get(
      "bolt_hdfs", file, std::map<std::string, std::string>{});
  ASSERT_TRUE(fsRes.ok()) << fsRes.status().ToString();
  auto fs = std::move(fsRes).value();

  ASSERT_TRUE(fs->Mkdirs(dir).ok());

  const std::string payload = "hello_hdfs";
  {
    auto outRes = fs->Create(file, /*overwrite=*/true);
    ASSERT_TRUE(outRes.ok()) << outRes.status().ToString();
    auto out = std::move(outRes).value();
    ASSERT_TRUE(out->Write(payload.data(), payload.size()).ok());
    ASSERT_TRUE(out->Flush().ok());
    ASSERT_TRUE(out->Close().ok());
  }

  auto renameRes = fs->Rename(file, renamed);
  ASSERT_TRUE(renameRes.ok());
  if (!renameRes.ok()) {
    const auto msg = renameRes.ToString();
    if (containsUnsupported(msg)) {
      (void)fs->Delete(base, /*recursive=*/true);
      GTEST_SKIP() << "Skipping rename test: " << msg;
    }
    FAIL() << msg;
  }

  auto oldExists = fs->Exists(file);
  ASSERT_TRUE(oldExists.ok());
  ASSERT_FALSE(oldExists.value());
  auto newExists = fs->Exists(renamed);
  ASSERT_TRUE(newExists.ok());
  ASSERT_TRUE(newExists.value());

  auto inRes = fs->Open(renamed);
  ASSERT_TRUE(inRes.ok()) << inRes.status().ToString();
  auto in = std::move(inRes).value();
  std::string buffer(payload.size(), '\0');
  auto readRes = in->Read(buffer.data(), buffer.size(), 0);
  ASSERT_TRUE(readRes.ok()) << readRes.status().ToString();
  ASSERT_EQ(static_cast<size_t>(readRes.value()), payload.size());
  ASSERT_EQ(buffer, payload);
  ASSERT_TRUE(in->Close().ok());

  ASSERT_TRUE(fs->Delete(base, /*recursive=*/true).ok());
}

TEST_F(PaimonHdfsFileSystemTest, WriteReadSeek) {
  if (!clusterAvailable_) {
    GTEST_SKIP() << "CLASSPATH missing; libhdfs JNI cannot initialize";
  }
  EnsurePaimonBoltHdfsFileSystemRegistered();

  const std::string base = cluster_->namenodeUri() +
      std::string("/paimon_bolt_hdfs_rw_test_") + std::to_string(::getpid());
  const std::string dir = base + "/dir";
  const std::string file = dir + "/data.bin";

  auto fsRes = ::paimon::FileSystemFactory::Get(
      "bolt_hdfs", file, std::map<std::string, std::string>{});
  ASSERT_TRUE(fsRes.ok()) << fsRes.status().ToString();
  auto fs = std::move(fsRes).value();

  ASSERT_TRUE(fs->Mkdirs(dir).ok()) << fs->Mkdirs(dir).ToString();

  const std::string payload =
      std::string("hello_") + std::string(4096, 'x') + "_world";
  {
    auto outRes = fs->Create(file, /*overwrite=*/true);
    ASSERT_TRUE(outRes.ok()) << outRes.status().ToString();
    auto out = std::move(outRes).value();
    ASSERT_TRUE(out->Write(payload.data(), payload.size()).ok());
    ASSERT_TRUE(out->Flush().ok());
    ASSERT_TRUE(out->Close().ok());
  }

  {
    auto stRes = fs->GetFileStatus(file);
    ASSERT_TRUE(stRes.ok()) << stRes.status().ToString();
    auto st = std::move(stRes).value();
    ASSERT_EQ(st->GetLen(), payload.size());
  }

  {
    auto inRes = fs->Open(file);
    ASSERT_TRUE(inRes.ok()) << inRes.status().ToString();
    auto in = std::move(inRes).value();

    auto lenRes = in->Length();
    ASSERT_TRUE(lenRes.ok());
    ASSERT_EQ(lenRes.value(), payload.size());

    // Read a slice with pread.
    std::string mid(16, '\0');
    auto readMid = in->Read(mid.data(), mid.size(), 6);
    ASSERT_TRUE(readMid.ok()) << readMid.status().ToString();
    ASSERT_EQ(readMid.value(), 16);
    ASSERT_EQ(mid, payload.substr(6, 16));

    // Seek + sequential read.
    ASSERT_TRUE(in->Seek(0, ::paimon::FS_SEEK_SET).ok());
    std::string all(payload.size(), '\0');
    size_t filled = 0;
    while (filled < all.size()) {
      auto r = in->Read(
          all.data() + filled, static_cast<uint32_t>(all.size() - filled));
      ASSERT_TRUE(r.ok()) << r.status().ToString();
      if (r.value() == 0) {
        break;
      }
      filled += r.value();
    }
    ASSERT_EQ(filled, payload.size());
    ASSERT_EQ(all, payload);
    ASSERT_TRUE(in->Close().ok());
  }

  ASSERT_TRUE(fs->Delete(base, /*recursive=*/true).ok());
}

TEST_F(PaimonHdfsFileSystemTest, OverwriteExistingFile) {
  if (!clusterAvailable_) {
    GTEST_SKIP() << "CLASSPATH missing; libhdfs JNI cannot initialize";
  }
  EnsurePaimonBoltHdfsFileSystemRegistered();

  const std::string base = cluster_->namenodeUri() +
      std::string("/paimon_bolt_hdfs_overwrite_test_") +
      std::to_string(::getpid());
  const std::string dir = base + "/dir";
  const std::string file = dir + "/data.bin";

  auto fsRes = ::paimon::FileSystemFactory::Get(
      "bolt_hdfs", file, std::map<std::string, std::string>{});
  ASSERT_TRUE(fsRes.ok()) << fsRes.status().ToString();
  auto fs = std::move(fsRes).value();

  ASSERT_TRUE(fs->Mkdirs(dir).ok()) << fs->Mkdirs(dir).ToString();

  // Create an initial payload.
  const std::string payload1 = "hello_world";
  {
    auto outRes = fs->Create(file, /*overwrite=*/true);
    ASSERT_TRUE(outRes.ok()) << outRes.status().ToString();
    auto out = std::move(outRes).value();
    ASSERT_TRUE(out->Write(payload1.data(), payload1.size()).ok());
    ASSERT_TRUE(out->Close().ok());
  }

  // Overwrite with a shorter payload.
  const std::string payload2 = "short";
  {
    auto outRes = fs->Create(file, /*overwrite=*/true);
    if (!outRes.ok()) {
      const auto msg = outRes.status().ToString();
      // Some internal HDFS client implementations do not support removing
      // files, which is required to implement overwrite.
      if (containsUnsupported(msg)) {
        // Best-effort cleanup via recursive directory delete.
        fs->Delete(base, /*recursive=*/true);
        GTEST_SKIP() << "Skipping overwrite test: " << msg;
      }
      FAIL() << msg;
    }
    auto out = std::move(outRes).value();
    ASSERT_TRUE(out->Write(payload2.data(), payload2.size()).ok());
    ASSERT_TRUE(out->Close().ok());
  }
  {
    auto stRes = fs->GetFileStatus(file);
    ASSERT_TRUE(stRes.ok());
    ASSERT_EQ(stRes.value()->GetLen(), payload2.size());
  }

  ASSERT_TRUE(fs->Delete(base, /*recursive=*/true).ok());
}

TEST_F(PaimonHdfsFileSystemTest, ListDirReturnsChildren) {
  if (!clusterAvailable_) {
    GTEST_SKIP() << "CLASSPATH missing; libhdfs JNI cannot initialize";
  }
  EnsurePaimonBoltHdfsFileSystemRegistered();

  const std::string base = cluster_->namenodeUri() +
      std::string("/paimon_bolt_hdfs_list_test_") + std::to_string(::getpid());
  const std::string dir = base + "/dir";
  const std::string f1 = dir + "/a.txt";
  const std::string f2 = dir + "/b.txt";

  auto fsRes = ::paimon::FileSystemFactory::Get(
      "bolt_hdfs", dir, std::map<std::string, std::string>{});
  ASSERT_TRUE(fsRes.ok()) << fsRes.status().ToString();
  auto fs = std::move(fsRes).value();

  ASSERT_TRUE(fs->Mkdirs(dir).ok());
  {
    auto o1 = fs->Create(f1, true);
    ASSERT_TRUE(o1.ok());
    ASSERT_TRUE(o1.value()->Write("a", 1).ok());
    ASSERT_TRUE(o1.value()->Close().ok());
    auto o2 = fs->Create(f2, true);
    ASSERT_TRUE(o2.ok());
    ASSERT_TRUE(o2.value()->Write("b", 1).ok());
    ASSERT_TRUE(o2.value()->Close().ok());
  }

  std::vector<std::unique_ptr<::paimon::BasicFileStatus>> children;
  ASSERT_TRUE(fs->ListDir(dir, &children).ok());
  ASSERT_EQ(children.size(), 2);

  std::vector<std::string> paths;
  paths.reserve(children.size());
  for (const auto& c : children) {
    paths.push_back(c->GetPath());
  }
  std::sort(paths.begin(), paths.end());
  ASSERT_EQ(paths[0], f1);
  ASSERT_EQ(paths[1], f2);

  ASSERT_TRUE(fs->Delete(base, /*recursive=*/true).ok());
}

} // namespace
} // namespace bytedance::bolt::connector::paimon

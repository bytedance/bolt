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

#include <folly/ScopeGuard.h>
#include <filesystem>
#include <fstream>
#include <future>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/exec/task_manager/Utils.h"

#include "bolt/exec/task_manager/TaskManager.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::exec::test {
namespace {
// Delegate real file operations, allowing tests to stall or fail only mkdir.
class SpillFileSystem : public filesystems::FileSystem {
 public:
  SpillFileSystem() : FileSystem(nullptr) {
    filesystems::registerLocalFileSystem();
    local_ = filesystems::getFileSystem("/", nullptr);
  }

  std::function<void(std::string_view)> onMkdir;

  std::string name() const override {
    return "task-manager-test";
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const filesystems::FileOptions& options) override {
    return local_->openFileForRead(path.substr(20), options);
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const filesystems::FileOptions& options) override {
    return local_->openFileForWrite(path.substr(20), options);
  }

  void remove(std::string_view path) override {
    local_->remove(path.substr(20));
  }
  void rename(std::string_view from, std::string_view to, bool overwrite)
      override {
    local_->rename(from.substr(20), to.substr(20), overwrite);
  }
  bool exists(std::string_view path) override {
    return local_->exists(path.substr(20));
  }
  std::vector<std::string> list(std::string_view path) override {
    return local_->list(path.substr(20));
  }
  void mkdir(std::string_view path) override {
    if (onMkdir) {
      onMkdir(path);
    }
    local_->mkdir(path.substr(20));
  }
  void rmdir(std::string_view path) override {
    local_->rmdir(path.substr(20));
  }

 private:
  std::shared_ptr<filesystems::FileSystem> local_;
};

class TaskManagerTest : public testing::Test,
                        public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::initialize({});
    }
  }

  TaskManagerOptions options() {
    TaskManagerOptions result;
    result.cpuThreadNumber = 2;
    result.ioThreadNumber = 1;
    result.useHiveConnector = false;
    result.useParquet = false;
    result.useHdfsFilesystem = false;
    result.useLocalFilesystem = true;
    result.periodCleanOldTasksMs = 0;
    return result;
  }

  std::shared_ptr<SpillFileSystem> spillFileSystem(const std::string& path) {
    auto fs = std::make_shared<SpillFileSystem>();
    filesystems::registerFileSystem(
        [path](std::string_view candidate) {
          return candidate.substr(0, path.size()) == path;
        },
        [fs](auto, auto) { return fs; });
    return fs;
  }

  core::PlanFragment fragment(size_t batches = 1) {
    auto data = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
    core::PlanFragment result;
    result.planNode = PlanBuilder()
                          .values(std::vector<RowVectorPtr>(batches, data))
                          .planNode();
    return result;
  }
};

TEST_F(TaskManagerTest, slowSpillCreationDoesNotBlockManager) {
  auto directory = TempDirectoryPath::create();
  auto opts = options();
  opts.spillDir = "task-manager-test://" + directory->getPath();
  auto fs = spillFileSystem(opts.spillDir);
  SCOPE_EXIT {
    fs->onMkdir = nullptr;
  };
  TaskManager manager(opts, {});
  auto existing =
      manager.createTask("existing", fragment(100), {}, {}, {}, 1, {}, 1);
  const auto plan = fragment();
  std::promise<void> entered;
  std::promise<void> resume;
  auto resumed = resume.get_future();
  std::atomic<bool> first{true};
  std::string spillPath;
  fs->onMkdir = [&](std::string_view path) {
    if (first.exchange(false)) {
      spillPath = path;
      entered.set_value();
      resumed.wait();
    }
  };
  auto creation = std::async(std::launch::async, [&] {
    EXPECT_ANY_THROW(manager.createTask("slow", plan));
  });
  entered.get_future().wait();
  auto operations = std::async(std::launch::async, [&] {
    EXPECT_EQ(manager.getTask("slow"), nullptr);
    EXPECT_EQ(manager.getTask("existing"), existing);
    EXPECT_EQ(manager.getNumTasks(), 1);
    EXPECT_EQ(manager.tasks().size(), 1);
    EXPECT_FALSE(manager.toString().empty());
    EXPECT_ANY_THROW(manager.createTask("slow", plan));
    auto other = manager.createTask("other", plan);
    EXPECT_TRUE(other->moveNext());
    EXPECT_FALSE(other->moveNext());
    EXPECT_EQ(manager.deleteTask("existing"), 0);
    manager.shutdown();
    EXPECT_EQ(manager.getNumTasks(), 0);
  });
  const auto status = operations.wait_for(std::chrono::seconds(2));
  // Always release mkdir before assertions or joining futures, even on failure.
  resume.set_value();
  operations.get();
  creation.get();
  EXPECT_EQ(status, std::future_status::ready);
  EXPECT_FALSE(fs->exists(spillPath));
}

#ifndef NDEBUG
TEST_F(TaskManagerTest, shutdownDuringStartupDoesNotPublishTask) {
  BOLT_TEST_VALUE_ENABLE();
  TaskManager manager(options(), {});
  auto existing = manager.createTask("existing", fragment());
  const auto plan = fragment(100);
  std::promise<void> entered;
  std::promise<void> resume;
  auto resumed = resume.get_future();
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::exec::Task::createDriversLocked",
      std::function<void(Task*)>([&](Task* task) {
        if (task->taskId() == "starting") {
          entered.set_value();
          resumed.wait();
        }
      }));
  auto creation = std::async(std::launch::async, [&] {
    EXPECT_ANY_THROW(
        manager.createTask("starting", plan, {}, {}, {}, 1, {}, 1));
  });
  entered.get_future().wait();
  auto shutdown = std::async(std::launch::async, [&] { manager.shutdown(); });
  auto inspection = std::async(std::launch::async, [&] {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (manager.getNumTasks() != 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(manager.getNumTasks(), 0);
    EXPECT_EQ(manager.getTask("starting"), nullptr);
    EXPECT_ANY_THROW(manager.createTask("late", plan));
  });
  const auto status = inspection.wait_for(std::chrono::seconds(3));
  resume.set_value();
  inspection.get();
  creation.get();
  shutdown.get();
  EXPECT_EQ(status, std::future_status::ready);
  EXPECT_EQ(manager.getTask("starting"), nullptr);
}
#endif

TEST_F(TaskManagerTest, failedCreationReleasesTaskId) {
  auto directory = TempDirectoryPath::create();
  auto opts = options();
  opts.spillDir = "task-manager-test://" + directory->getPath();
  auto fs = spillFileSystem(opts.spillDir);
  SCOPE_EXIT {
    fs->onMkdir = nullptr;
  };
  TaskManager manager(opts, {});
  fs->onMkdir = [](std::string_view) { BOLT_FAIL("mkdir failed"); };
  EXPECT_ANY_THROW(manager.createTask("retry", fragment()));
  EXPECT_EQ(manager.getTask("retry"), nullptr);
  EXPECT_TRUE(fs->list(opts.spillDir).empty());
  fs->onMkdir = nullptr;
  EXPECT_ANY_THROW(manager.createTask("retry", fragment(), {}, {}, {}, 0));
  EXPECT_EQ(manager.getTask("retry"), nullptr);
  EXPECT_TRUE(fs->list(opts.spillDir).empty());
  const auto plan = fragment(100);
  EXPECT_ANY_THROW(manager.createTask(
      "retry", plan, {}, {}, {{"unknown-node", {}}}, 1, {}, 1));
  EXPECT_EQ(manager.getTask("retry"), nullptr);
  // Cancellation completes before the executor callback drops its last task
  // reference. Spill cleanup follows destruction of that final reference.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!fs->list(opts.spillDir).empty() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(fs->list(opts.spillDir).empty());
  auto task = manager.createTask("retry", fragment());
  EXPECT_TRUE(task->moveNext());
  EXPECT_FALSE(task->moveNext());
}

TEST_F(TaskManagerTest, splitAcceptsRegisteredHiveConnector) {
  const std::string id = "custom-task-manager-hive";
  connector::registerConnector(std::make_shared<connector::hive::HiveConnector>(
      id,
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{}),
      nullptr));
  SCOPE_EXIT {
    connector::unregisterConnector(id);
  };
  auto split =
      makeSplit(id, "/test.parquet", dwio::common::FileFormat::PARQUET, 7, 11);
  auto hive = std::dynamic_pointer_cast<connector::hive::HiveConnectorSplit>(
      split.connectorSplit);
  ASSERT_NE(hive, nullptr);
  EXPECT_EQ(hive->connectorId, id);
  EXPECT_EQ(hive->filePath, "/test.parquet");
  EXPECT_EQ(hive->start, 7);
  EXPECT_EQ(hive->length, 11);
  EXPECT_ANY_THROW(makeSplit(
      "unregistered", "/test.parquet", dwio::common::FileFormat::PARQUET));
}

TEST_F(TaskManagerTest, splitRejectsNonHiveConnector) {
  const auto& id = WrappedTask::kHiveConnectorId;
  connector::registerConnector(
      std::make_shared<connector::tpch::TpchConnector>(id, nullptr, nullptr));
  SCOPE_EXIT {
    connector::unregisterConnector(id);
  };
  EXPECT_ANY_THROW(
      makeSplit(id, "/test.parquet", dwio::common::FileFormat::PARQUET));
}

#ifdef BOLT_ENABLE_PARQUET
TEST_F(TaskManagerTest, omittedSplitLengthReadsWholeFile) {
  const auto& id = WrappedTask::kHiveConnectorId;
  connector::registerConnector(std::make_shared<connector::hive::HiveConnector>(
      id,
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{}),
      nullptr));
  SCOPE_EXIT {
    connector::unregisterConnector(id);
  };
  auto opts = options();
  opts.useParquet = true;
  TaskManager manager(opts, {});
  const auto path = bolt::test::getDataFilePath(
      "../../../dwio/parquet/tests/examples/sample.parquet");
  core::PlanFragment plan;
  plan.planNode = PlanBuilder().tableScan(ROW({"a"}, {BIGINT()})).planNode();
  for (int i = 0; i < 3; ++i) {
    auto task = manager.createTask(std::to_string(i), plan);
    if (i == 0) {
      task->addSplit(makeSplit(id, path, dwio::common::FileFormat::PARQUET));
    } else if (i == 1) {
      task->addHiveFile("0", path, dwio::common::FileFormat::PARQUET);
    } else {
      // An explicitly empty range must retain its meaning.
      task->addHiveFile("0", path, dwio::common::FileFormat::PARQUET, 0, 0);
    }
    task->noMoreSplits();
    size_t rows = 0;
    while (task->moveNext()) {
      rows += task->currentResult()->size();
    }
    EXPECT_EQ(rows, i == 2 ? 0 : 20);
  }
}
#endif

TEST_F(TaskManagerTest, spillDirectoriesAreIsolated) {
  auto directory = TempDirectoryPath::create();
  auto opts = options();
  opts.spillDir = directory->getPath();
  TaskManager manager(opts, {});
  auto first = manager.createTask("first", fragment());
  auto second = manager.createTask("second", fragment());
  const auto firstPath = first->getTask()->spillDirectory();
  const auto secondPath = second->getTask()->spillDirectory();
  ASSERT_NE(firstPath, secondPath);
  ASSERT_NE(firstPath, opts.spillDir);
  ASSERT_NE(secondPath, opts.spillDir);
  EXPECT_TRUE(std::filesystem::exists(firstPath));
  EXPECT_TRUE(std::filesystem::exists(secondPath));
  std::ofstream(secondPath + "/sentinel") << "keep";
  while (first->moveNext()) {
  }
  first->getTask()->taskCompletionFuture().wait();
  first->getTask()->requestCancel().wait();
  // Task::leave fulfills the finish future before Driver::run returns and its
  // executor callback releases DriverCtx::task. Cleanup requires that final
  // driver reference to be gone as well.
  std::weak_ptr<Task> firstTask = first->getTask();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (firstTask.use_count() > 1 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(firstTask.use_count(), 1);
  first.reset();
  ASSERT_EQ(manager.deleteTask("first", true), 0);
  EXPECT_FALSE(std::filesystem::exists(firstPath));
  EXPECT_TRUE(std::filesystem::exists(secondPath + "/sentinel"));
  EXPECT_TRUE(std::filesystem::exists(opts.spillDir));
}

TEST_F(TaskManagerTest, shutdownAfterPartialConsumption) {
  auto manager = std::make_unique<TaskManager>(
      options(), memory::MemoryManager::Options{});
  auto task =
      manager->createTask("partial", fragment(100), {}, {}, {}, 1, {}, 1);
  ASSERT_TRUE(task->moveNext());
  manager.reset();
  EXPECT_NE(task->getTask()->state(), TaskState::kRunning);
  task.reset();
}

TEST_F(TaskManagerTest, rowVectorOutlivesManager) {
  RowVectorPtr vector;
  {
    TaskManager manager(options(), {});
    auto task = manager.createTask("complete", fragment());
    ASSERT_TRUE(task->moveNext());
    vector = task->currentResult();
    EXPECT_FALSE(task->moveNext());
  }
  EXPECT_EQ(vector->childAt(0)->as<FlatVector<int64_t>>()->valueAt(2), 3);
}

TEST_F(TaskManagerTest, arrowBatchOutlivesManager) {
  std::shared_ptr<arrow::RecordBatch> batch;
  {
    TaskManager manager(options(), {});
    auto task = manager.createTask("complete", fragment());
    ASSERT_TRUE(task->moveNext());
    batch = task->currentArrowResult();
    EXPECT_FALSE(task->moveNext());
  }
  EXPECT_EQ(batch->num_rows(), 3);
}

TEST_F(TaskManagerTest, arrowArrayOutlivesManager) {
  ArrowSchema schema{};
  ArrowArray array{};
  {
    TaskManager manager(options(), {});
    auto task = manager.createTask("complete", fragment());
    ASSERT_TRUE(task->moveNext());
    ASSERT_TRUE(task->convertCurrentToArrow(schema, array));
    EXPECT_FALSE(task->moveNext());
  }
  EXPECT_EQ(array.length, 3);
  EXPECT_EQ(static_cast<const int64_t*>(array.children[0]->buffers[1])[2], 3);
  schema.release(&schema);
  array.release(&array);
}

TEST_F(TaskManagerTest, copiedSizeControlsBackpressure) {
  auto outputPool = memory::memoryManager()->addLeafPool();
  auto values = makeFlatVector<int64_t>({7});
  auto input = makeRowVector({BaseVector::wrapInConstant(10000, 0, values)});
  const auto sourceBytes = input->retainedSize();
  BoltVectorQueue queue(outputPool, sourceBytes + 1);
  ContinueFuture future;
  EXPECT_EQ(queue.enqueue(input, &future), BlockingReason::kWaitForConsumer);
  auto output = queue.dequeue();
  EXPECT_GT(output->retainedSize(), sourceBytes + 1);
  EXPECT_TRUE(future.isReady());
}

TEST_F(TaskManagerTest, nestedStringsOutliveSourcePool) {
  const std::string value(128, 'x');
  auto outputPool = memory::memoryManager()->addLeafPool();
  auto queue = std::make_unique<BoltVectorQueue>(outputPool, 1024 * 1024);
  {
    auto sourcePool = memory::memoryManager()->addLeafPool();
    bolt::test::VectorMaker maker(sourcePool.get());
    auto input =
        maker.rowVector({maker.arrayVector<StringView>({{StringView(value)}})});
    ContinueFuture future;
    EXPECT_EQ(queue->enqueue(input, &future), BlockingReason::kNotBlocked);
    input.reset();
    EXPECT_EQ(sourcePool->usedBytes(), 0);
  }
  auto output = queue->dequeue();
  queue.reset();
  outputPool.reset();
  auto* elements = output->childAt(0)
                       ->as<ArrayVector>()
                       ->elements()
                       ->as<FlatVector<StringView>>();
  EXPECT_EQ(elements->valueAt(0).str(), value);
}

TEST_F(TaskManagerTest, failedTaskWakesConsumer) {
  TaskManager manager(options(), {});
  auto task = manager.createTask("failed", fragment(100), {}, {}, {}, 1, {}, 1);
  ASSERT_TRUE(task->moveNext());
  task->getTask()->setError("test failure");
  EXPECT_ANY_THROW(task->moveNext());
}

TEST_F(TaskManagerTest, multipleManagersAndShutdownRejectsTasks) {
  TaskManager first(options(), {});
  TaskManager second(options(), {});
  first.shutdown();
  EXPECT_ANY_THROW(first.createTask("late", fragment()));
  auto task = second.createTask("valid", fragment());
  ASSERT_TRUE(task->moveNext());
  EXPECT_FALSE(task->moveNext());
}

TEST_F(TaskManagerTest, closingQueueWakesConsumer) {
  auto pool = memory::memoryManager()->addLeafPool();
  BoltVectorQueue queue(pool, 1);
  auto result = std::async(std::launch::async, [&] { return queue.dequeue(); });
  queue.close(true);
  ASSERT_EQ(
      result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(result.get(), nullptr);
}

TEST_F(TaskManagerTest, tinyQueueUnblocksProducer) {
  auto pool = memory::memoryManager()->addLeafPool();
  BoltVectorQueue queue(pool, 1);
  queue.setNumProducers(1);
  ContinueFuture future;
  EXPECT_EQ(
      queue.enqueue(makeRowVector({makeFlatVector<int64_t>({1})}), &future),
      BlockingReason::kWaitForConsumer);
  EXPECT_NE(queue.dequeue(), nullptr);
  EXPECT_TRUE(future.isReady());
  queue.close();
  EXPECT_EQ(queue.dequeue(), nullptr);
}
} // namespace
} // namespace bytedance::bolt::exec::test

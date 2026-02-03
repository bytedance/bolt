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

#include "bolt/common/base/AsyncSource.h"
#include "bolt/common/base/RuntimeMetrics.h"
#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/caching/ScanTracker.h"
#include "bolt/common/future/BoltPromise.h"
// #include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/core/ExpressionEvaluator.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/vector/ComplexVector.h"

#include <folly/Synchronized.h>
#include <atomic>
#include <cstdint>
#include <mutex>
namespace bytedance::bolt {
class Config;
}
namespace bytedance::bolt::wave {
class WaveDataSource;
}
namespace bytedance::bolt::common {
class Filter;
}
namespace bytedance::bolt::config {
class ConfigBase;
}
namespace bytedance::bolt::core {
class QueryConfig;
}
namespace bytedance::bolt::core {
class ITypedExpr;
}
namespace bytedance::bolt::connector {

class DataSource;

// A split represents a chunk of data that a connector should load and return
// as a RowVectorPtr, potentially after processing pushdowns.
struct ConnectorSplit : public ISerializable {
  const std::string connectorId;
  const int64_t splitWeight{0};

  std::unique_ptr<AsyncSource<DataSource>> dataSource;

  explicit ConnectorSplit(const std::string& _connectorId)
      : connectorId(_connectorId) {}

  folly::dynamic serialize() const override {
    BOLT_UNSUPPORTED();
    return nullptr;
  }

  virtual ~ConnectorSplit() {}

  virtual bool isFinished() const {
    BOLT_NYI("isFinished is not supported yet");
  }

  virtual std::string toString() const {
    return fmt::format("[split: {}]", connectorId);
  }
};

class ColumnHandle : public ISerializable {
 public:
  virtual ~ColumnHandle() = default;

  folly::dynamic serialize() const override;

 protected:
  static folly::dynamic serializeBase(std::string_view name);
};

using ColumnHandlePtr = std::shared_ptr<const ColumnHandle>;

class ConnectorTableHandle : public ISerializable {
 public:
  explicit ConnectorTableHandle(std::string connectorId)
      : connectorId_(std::move(connectorId)) {}

  virtual ~ConnectorTableHandle() = default;

  virtual std::string toString() const {
    BOLT_NYI();
  }

  const std::string& connectorId() const {
    return connectorId_;
  }

  /// Returns the connector-dependent table name. Used with
  /// ConnectorMetadata. Implementations need to supply a definition
  /// to work with metadata.
  virtual const std::string& name() const {
    BOLT_UNSUPPORTED();
  }

  /// Returns true if the connector table handle supports index lookup.
  virtual bool supportsIndexLookup() const {
    return false;
  }

  virtual folly::dynamic serialize() const override;

 protected:
  folly::dynamic serializeBase(std::string_view name) const;

 private:
  const std::string connectorId_;
};

using ConnectorTableHandlePtr = std::shared_ptr<const ConnectorTableHandle>;

/**
 * Represents a request for writing to connector
 */
class ConnectorInsertTableHandle : public ISerializable {
 public:
  virtual ~ConnectorInsertTableHandle() {}

  // Whether multi-threaded write is supported by this connector. Planner uses
  // this flag to determine number of drivers.
  virtual bool supportsMultiThreading() const {
    return false;
  }

  folly::dynamic serialize() const override {
    BOLT_NYI();
  }
};

/// Represents the commit strategy for writing to connector.
enum class CommitStrategy {
  kNoCommit, // No more commit actions are needed.
  kTaskCommit // Task level commit is needed.
};

/// Return a string encoding of the given commit strategy.
std::string commitStrategyToString(CommitStrategy commitStrategy);

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    CommitStrategy strategy) {
  os << commitStrategyToString(strategy);
  return os;
}

/// Return a commit strategy of the given string encoding.
CommitStrategy stringToCommitStrategy(const std::string& strategy);

/// Writes data received from table writer operator into different partitions
/// based on the specific table layout. The actual implementation doesn't need
/// to be thread-safe.
class DataSink {
 public:
  struct Stats {
    uint64_t numWrittenBytes{0};
    uint32_t numWrittenFiles{0};
    common::SpillStats spillStats;

    bool empty() const;

    std::string toString() const;
  };

  virtual ~DataSink() = default;

  /// Add the next data (vector) to be written. This call is blocking.
  /// TODO maybe at some point we want to make it async.
  virtual void appendData(RowVectorPtr input) = 0;

  /// Returns the stats of this data sink.
  virtual Stats stats() const = 0;

  /// Called once after all data has been added via possibly multiple calls to
  /// appendData(). The function returns the metadata of written data in string
  /// form. We don't expect any appendData() calls on a closed data sink object.
  virtual std::vector<std::string> close() = 0;

  /// Called to abort this data sink object and we don't expect any appendData()
  /// calls on an aborted data sink object.
  virtual void abort() = 0;
};

class DataSource {
 public:
  static constexpr int64_t kUnknownRowSize = -1;
  virtual ~DataSource() = default;

  // Add split to process, then call next multiple times to process the split.
  // A split must be fully processed by next before another split can be
  // added. Next returns nullptr to indicate that current split is fully
  // processed.
  virtual void addSplit(std::shared_ptr<ConnectorSplit> split) = 0;

  // Process a split added via addSplit. Returns nullptr if split has been fully
  // processed. Returns std::nullopt and sets the 'future' if started
  // asynchronous work and needs to wait for it to complete to continue
  // processing. The caller will wait for the 'future' to complete before
  // calling 'next' again.
  virtual std::optional<RowVectorPtr> next(
      uint64_t size,
      bolt::ContinueFuture& future) = 0;

  // Add dynamically generated filter.
  // @param outputChannel index into outputType specified in
  // Connector::createDataSource() that identifies the column this filter
  // applies to.
  virtual void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) = 0;

  // Returns the number of input bytes processed so far.
  virtual uint64_t getCompletedBytes() = 0;

  // Returns the number of input rows processed so far.
  virtual uint64_t getCompletedRows() = 0;

  virtual std::vector<uint64_t> getCompletedBytesReads() {
    return {0, 0, 0, 0};
  }

  virtual std::vector<uint64_t> getCompletedCntReads() {
    return {0, 0, 0, 0};
  }

  virtual std::vector<uint64_t> getCompletedScanTimeReads() {
    return {0, 0, 0, 0};
  }

  virtual std::unordered_map<std::string, RuntimeCounter> runtimeStats() = 0;

  // Returns true if 'this' has initiated all the prefetch this will
  // initiate. This means that the caller should schedule next splits
  // to prefetch in the background. false if the source does not
  // prefetch.
  virtual bool allPrefetchIssued() const {
    return false;
  }

  virtual bool isFinished() const {
    BOLT_NYI("isFinished is not supported yet");
  }

  // Initializes this from 'source'. 'source' is effectively moved
  // into 'this' Adaptation like dynamic filters stay in effect but
  // the parts dealing with open files, prefetched data etc. are moved. 'source'
  // is freed after the move.
  virtual void setFromDataSource(std::unique_ptr<DataSource> /*source*/) {
    BOLT_UNSUPPORTED("setFromDataSource");
  }

  // Returns a connector dependent row size if available. This can be
  // called after addSplit().  This estimates uncompressed data
  // sizes. This is better than getCompletedBytes()/getCompletedRows()
  // since these track sizes before decompression and may include
  // read-ahead and extra IO from coalescing reads and  will not
  // fully account for size of sparsely accessed columns.
  virtual int64_t estimatedRowSize() {
    return kUnknownRowSize;
  }

  virtual void close() {}
};

class IndexSource {
 public:
  virtual ~IndexSource() = default;

  /// Represents a lookup request for a given input.
  struct LookupRequest {
    /// Contains the input column vectors used by lookup join and range
    /// conditions.
    RowVectorPtr input;

    explicit LookupRequest(RowVectorPtr input) : input(std::move(input)) {}
  };

  /// Represents the lookup result for a subset of input produced by the
  /// 'LookupResultIterator'.
  struct LookupResult {
    /// Specifies the indices of input row in the lookup request that have
    /// matches in 'output'. It contains the input indices in the order
    /// of the input rows in the lookup request. Any gap in the indices means
    /// the input rows that has no matches in output.
    ///
    /// Example:
    ///   LookupRequest: input = [0, 1, 2, 3, 4]
    ///   LookupResult:  inputHits = [0, 0, 2, 2, 3, 4, 4, 4]
    ///                  output    = [0, 1, 2, 3, 4, 5, 6, 7]
    ///
    ///   Here is match results for each input row:
    ///   input row #0: match with output rows #0 and #1.
    ///   input row #1: no matches
    ///   input row #2: match with output rows #2 and #3.
    ///   input row #3: match with output row #4.
    ///   input row #4: match with output rows #5, #6 and #7.
    ///
    /// 'LookupResultIterator' must also produce the output result in order of
    /// input rows.
    BufferPtr inputHits;

    /// Contains the lookup result rows.
    RowVectorPtr output;

    size_t size() const {
      return output->size();
    }

    LookupResult(BufferPtr _inputHits, RowVectorPtr _output)
        : inputHits(std::move(_inputHits)), output(std::move(_output)) {
      BOLT_CHECK_EQ(inputHits->size() / sizeof(vector_size_t), output->size());
    }
  };

  /// The lookup result iterator used to fetch the lookup result in batch for a
  /// given lookup request.
  class LookupResultIterator {
   public:
    virtual ~LookupResultIterator() = default;

    /// Invoked to fetch up to 'size' number of output rows. Returns nullptr if
    /// all the lookup results have been fetched. Returns std::nullopt and sets
    /// the 'future' if started asynchronous work and needs to wait for it to
    /// complete to continue processing. The caller will wait for the 'future'
    /// to complete before calling 'next' again.
    virtual std::optional<std::unique_ptr<LookupResult>> next(
        vector_size_t size,
        bolt::ContinueFuture& future) = 0;
  };

  virtual std::shared_ptr<LookupResultIterator> lookup(
      const LookupRequest& request) = 0;

  virtual std::unordered_map<std::string, RuntimeCounter> runtimeStats() = 0;
};

// recored in-flight async threads and wait them done
class AsyncThreadCtx {
 public:
  explicit AsyncThreadCtx(int64_t memLimit, bool adaptive)
      : preloadBytesLimit_(memLimit), adaptive_(adaptive) {
    BOLT_CHECK_GT(preloadBytesLimit_, 0);
  }

  void in() {
    std::unique_lock lock(mutex_);
    numIn_++;
    cv_.notify_one();
  }
  void out() {
    std::unique_lock lock(mutex_);
    numIn_--;
    cv_.notify_one();
  }

  void wait() {
    // check timeout and output warninglogs
    std::unique_lock lock(mutex_);
    cv_.wait_until(
        lock,
        std::chrono::steady_clock::now() + std::chrono::seconds(600),
        [this] { return numIn_ == 0; });
  }

  std::mutex& getMutex() {
    return mutex_;
  }

  int64_t& inPreloadingBytes() {
    return inPreloadingBytes_;
  }

  int64_t preloadBytesLimit() const {
    return preloadBytesLimit_;
  }

  void disallowPreload() {
    if (adaptive_ && allowPreload_.load()) {
      allowPreload_ = false;
      LOG(INFO) << "Disallow scan preload due to limited memory";
    }
  }

  bool allowPreload() {
    return allowPreload_.load();
  }

 private:
  std::mutex mutex_;
  int numIn_{0};
  int64_t inPreloadingBytes_{0};
  int64_t preloadBytesLimit_{0};
  std::condition_variable cv_;
  std::atomic_bool allowPreload_{true};
  bool adaptive_{true};
};

/// Collection of context data for use in a DataSource, IndexSource or DataSink.
/// One instance of this per DataSource and DataSink. This may be passed between
/// threads but methods must be invoked sequentially. Serializing use is the
/// responsibility of the caller.
class ConnectorQueryCtx {
 public:
  ConnectorQueryCtx(
      memory::MemoryPool* operatorPool,
      memory::MemoryPool* connectorPool,
      const config::ConfigBase* sessionProperties,
      const common::SpillConfig* spillConfig,
      connector::AsyncThreadCtx* const asyncThreadCtx,
      std::unique_ptr<core::ExpressionEvaluator> expressionEvaluator,
      cache::AsyncDataCache* cache,
      const std::string& queryId,
      const std::string& taskId,
      const std::string& planNodeId,
      int driverId)
      : operatorPool_(operatorPool),
        connectorPool_(connectorPool),
        sessionProperties_(sessionProperties),
        spillConfig_(spillConfig),
        asyncThreadCtx_(asyncThreadCtx),
        expressionEvaluator_(std::move(expressionEvaluator)),
        cache_(cache),
        scanId_(fmt::format("{}.{}", taskId, planNodeId)),
        queryId_(queryId),
        taskId_(taskId),
        driverId_(driverId),
        planNodeId_(planNodeId) {
    BOLT_CHECK_NOT_NULL(sessionProperties);
  }

  /// Returns the associated operator's memory pool which is a leaf kind of
  /// memory pool, used for direct memory allocation use.
  memory::MemoryPool* memoryPool() const {
    return operatorPool_;
  }

  /// Returns the connector's memory pool which is an aggregate kind of
  /// memory pool, used for the data sink for table write that needs the
  /// hierarchical memory pool management, such as HiveDataSink.
  memory::MemoryPool* connectorMemoryPool() const {
    return connectorPool_;
  }

  const config::ConfigBase* sessionProperties() const {
    return sessionProperties_;
  }

  const common::SpillConfig* spillConfig() const {
    return spillConfig_;
  }

  core::ExpressionEvaluator* expressionEvaluator() const {
    return expressionEvaluator_.get();
  }

  cache::AsyncDataCache* cache() const {
    return cache_;
  }

  /// This is a combination of task id and the scan's PlanNodeId. This is an
  /// id that allows sharing state between different threads of the same
  /// scan. This is used for locating a scanTracker, which tracks the read
  /// density of columns for prefetch and other memory hierarchy purposes.
  const std::string& scanId() const {
    return scanId_;
  }

  const std::string queryId() const {
    return queryId_;
  }

  const std::string& taskId() const {
    return taskId_;
  }

  int driverId() const {
    return driverId_;
  }

  const std::string& planNodeId() const {
    return planNodeId_;
  }

  AsyncThreadCtx* asyncThreadCtx() const {
    return asyncThreadCtx_;
  }

 private:
  memory::MemoryPool* const operatorPool_;
  memory::MemoryPool* const connectorPool_;
  const config::ConfigBase* const sessionProperties_;
  const common::SpillConfig* const spillConfig_;
  AsyncThreadCtx* const asyncThreadCtx_;
  std::unique_ptr<core::ExpressionEvaluator> expressionEvaluator_;
  cache::AsyncDataCache* cache_;
  const std::string scanId_;
  const std::string queryId_;
  const std::string taskId_;
  const int driverId_;
  const std::string planNodeId_;
};

class Connector {
 public:
  explicit Connector(const std::string& id) : id_(id) {}

  virtual ~Connector() = default;

  const std::string& connectorId() const {
    return id_;
  }

  virtual const std::shared_ptr<const config::ConfigBase>& connectorConfig()
      const {
    BOLT_NYI("connectorConfig is not supported yet");
  }

  /// Returns true if this connector would accept a filter dynamically
  /// generated during query execution.
  virtual bool canAddDynamicFilter() const {
    return false;
  }

  virtual std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& queryConfig) = 0;

  // Returns true if addSplit of DataSource can use 'dataSource' from
  // ConnectorSplit in addSplit(). If so, TableScan can preload splits
  // so that file opening and metadata operations are off the Driver'
  // thread.
  virtual bool supportsSplitPreload() {
    return false;
  }

  /// Returns true if the connector supports index lookup, otherwise false.
  virtual bool supportsIndexLookup() const {
    return false;
  }

  /// Creates index source for index join lookup.
  /// @param inputType The list of probe-side columns that either used in
  /// equi-clauses or join conditions.
  /// @param numJoinKeys The number of key columns used in join equi-clauses.
  /// The first 'numJoinKeys' columns in 'inputType' form a prefix of the
  /// index, and the rest of the columns in inputType are expected to be used in
  /// 'joinCondition'.
  /// @param joinConditions The join conditions. It expects inputs columns from
  /// the 'tail' of 'inputType' and from 'columnHandles'.
  /// @param outputType The lookup output type from index source.
  /// @param tableHandle The index table handle.
  /// @param columnHandles The column handles which maps from column name
  /// used in 'outputType' and 'joinConditions' to the corresponding column
  /// handles in the index table.
  /// @param connectorQueryCtx The query context.
  ///
  /// Here is an example that how the lookup join operator uses index source:
  ///
  /// SELECT t.sid, t.day_ts, u.event_value
  /// FROM t LEFT JOIN u
  /// ON t.sid = u.sid
  ///  AND contains(t.event_list, u.event_type)
  ///  AND t.ds BETWEEN '2024-01-01' AND '2024-01-07'
  ///
  /// Here,
  /// - 'inputType' is ROW{t.sid, t.event_list}
  /// - 'numJoinKeys' is 1 since only t.sid is used in join equi-clauses.
  /// - 'joinConditions' is list of one expression: contains(t.event_list,
  ///    u.event_type)
  /// - 'outputType' is ROW{u.event_value}
  /// - 'tableHandle' specifies the metadata of the index table.
  /// - 'columnHandles' is a map from 'u.event_type' (in 'joinConditions') and
  ///   'u.event_value' (in 'outputType') to the actual column names in the
  ///   index table.
  /// - 'connectorQueryCtx' provide the connector query execution context.
  ///
  virtual std::shared_ptr<IndexSource> createIndexSource(
      const RowTypePtr& inputType,
      size_t numJoinKeys,
      const std::vector<std::shared_ptr<const core::ITypedExpr>>&
          joinConditions,
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) {
    BOLT_UNSUPPORTED(
        "Connector {} does not support index source", connectorId());
  }

  virtual std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const core::QueryConfig& queryConfig) = 0;

  // Returns a ScanTracker for 'id'. 'id' uniquely identifies the
  // tracker and different threads will share the same
  // instance. 'loadQuantum' is the largest single IO for the query
  // being tracked.
  static std::shared_ptr<cache::ScanTracker> getTracker(
      const std::string& scanId,
      int32_t loadQuantum);

  virtual folly::Executor* FOLLY_NULLABLE executor() const {
    return nullptr;
  }

 private:
  static void unregisterTracker(cache::ScanTracker* tracker);

  const std::string id_;

  static folly::Synchronized<
      std::unordered_map<std::string_view, std::weak_ptr<cache::ScanTracker>>>
      trackers_;
};

class ConnectorFactory {
 public:
  explicit ConnectorFactory(const char* name) : name_(name) {}

  virtual ~ConnectorFactory() = default;

  // Initialize is called during the factory registration.
  virtual void initialize() {}

  const std::string& connectorName() const {
    return name_;
  }

  virtual std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) = 0;

  // TODO(jtan6): [Config Refactor] Remove this old API when refactor is done.
  virtual std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) = 0;

 private:
  const std::string name_;
};

/// Adds a factory for creating connectors to the registry using connector
/// name as the key. Throws if factor with the same name is already present.
/// Always returns true. The return value makes it easy to use with
/// FB_ANONYMOUS_VARIABLE.
bool registerConnectorFactory(std::shared_ptr<ConnectorFactory> factory);

/// Returns true if a connector with the specified name has been registered,
/// false otherwise.
bool hasConnectorFactory(const std::string& connectorName);

/// Unregister a connector factory by name.
/// Returns true if a connector with the specified name has been
/// unregistered, false otherwise.
bool unregisterConnectorFactory(const std::string& connectorName);

/// Returns a factory for creating connectors with the specified name.
/// Throws if factory doesn't exist.
std::shared_ptr<ConnectorFactory> getConnectorFactory(
    const std::string& connectorName);

/// Adds connector instance to the registry using connector ID as the key.
/// Throws if connector with the same ID is already present. Always returns
/// true. The return value makes it easy to use with FB_ANONYMOUS_VARIABLE.
bool registerConnector(std::shared_ptr<Connector> connector);

/// Removes the connector with specified ID from the registry. Returns true
/// if connector was removed and false if connector didn't exist.
bool unregisterConnector(const std::string& connectorId);

/// Returns a connector with specified ID. Throws if connector doesn't
/// exist.
std::shared_ptr<Connector> getConnector(const std::string& connectorId);

/// Returns true if connector with specified ID is already registered.
bool isConnectorRegistered(const std::string& connectorId);

/// Returns a map of all (connectorId -> connector) pairs currently registered.
const std::unordered_map<std::string, std::shared_ptr<Connector>>&
getAllConnectors();

#define BOLT_REGISTER_CONNECTOR_FACTORY(theFactory)                       \
  namespace {                                                             \
  static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) =                 \
      bytedance::bolt::connector::registerConnectorFactory((theFactory)); \
  }
} // namespace bytedance::bolt::connector

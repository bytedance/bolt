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

#include <fmt/format.h>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>

#include "bolt/common/memory/sparksql/KnownNameAndStats.h"
#include "bolt/common/memory/sparksql/MemoryConsumer.h"
#include "bolt/common/memory/sparksql/MemoryUsageStats.h"
#include "bolt/common/memory/sparksql/SpillerPhase.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
#include "bolt/common/memory/sparksql/WeakPtrHelper.h"
namespace bytedance::bolt::memory::sparksql {

class Spiller;
using SpillerPtr = std::shared_ptr<Spiller>;
using SpillerWeakPtr = std::weak_ptr<Spiller>;

class MemoryTarget;
using MemoryTargetPtr = std::shared_ptr<MemoryTarget>;
using MemoryTargetWeakPtr = std::weak_ptr<MemoryTarget>;

class TreeMemoryTarget;
using TreeMemoryTargetPtr = std::shared_ptr<TreeMemoryTarget>;
using TreeMemoryTargetWeakPtr = std::weak_ptr<TreeMemoryTarget>;

class ConsumerTargetBridge;
using ConsumerTargetBridgePtr = std::shared_ptr<ConsumerTargetBridge>;
using ConsumerTargetBridgeWeakPtr = std::weak_ptr<ConsumerTargetBridge>;

class TreeMemoryTargetNode;
using TreeMemoryTargetNodePtr = std::shared_ptr<TreeMemoryTargetNode>;
using TreeMemoryTargetNodeWeakPtr = std::weak_ptr<TreeMemoryTargetNode>;

class ConsumerTargetBridgeFactory;
using ConsumerTargetBridgeFactoryPtr =
    std::shared_ptr<ConsumerTargetBridgeFactory>;

/* Super class in Gluten*/
class MemoryTarget {
 public:
  virtual ~MemoryTarget() = default;

  virtual int64_t borrow(int64_t size) = 0;

  virtual int64_t repay(int64_t size) = 0;

  virtual int64_t usedBytes() = 0;

  virtual std::string toString() = 0;
};

/* The base class for tree management structures. */
class TreeMemoryTarget : public MemoryTarget, public KnownNameAndStats {
 public:
  static constexpr int64_t kCapacityUnlimited = INT64_MAX;

  ~TreeMemoryTarget() override = default;

  virtual TreeMemoryTargetPtr newChild(
      const std::string& name,
      const int64_t capacity,
      const std::list<SpillerPtr>& spillers,
      const std::map<std::string, MemoryUsageStatsBuilderPtr>&
          virtualChildren) = 0;

  virtual const std::map<std::string, TreeMemoryTargetPtr>& children() = 0;

  virtual TreeMemoryTargetWeakPtr parent() = 0;

  virtual const std::list<SpillerPtr>& getSpillers() = 0;

  virtual void appendSpiller(SpillerPtr& spiller) = 0;
};

class TreeMemoryTargetNode final
    : public TreeMemoryTarget,
      public std::enable_shared_from_this<TreeMemoryTargetNode> {
 public:
  TreeMemoryTargetNode(
      TreeMemoryTargetWeakPtr parent,
      const std::string& name,
      const int64_t capacity,
      const std::list<SpillerPtr>& spillers,
      const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren);

  ~TreeMemoryTargetNode() override;

  TreeMemoryTargetPtr newChild(
      const std::string& name,
      const int64_t capacity,
      const std::list<SpillerPtr>& spillers,
      const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren)
      override;

  int64_t borrow(int64_t size) override;

  const std::list<SpillerPtr>& getSpillers() override;

  void appendSpiller(SpillerPtr& spiller) override;

  int64_t repay(int64_t size) override;

  int64_t usedBytes() override;

  const std::string& name() override;

  MemoryUsageStatsPtr stats() override;

  const std::map<std::string, TreeMemoryTargetPtr>& children() override;

  TreeMemoryTargetWeakPtr parent() override;

  std::string toString() override;

  friend std::ostream& operator<<(
      std::ostream& os,
      const TreeMemoryTargetNode& node);

  friend std::ostream& operator<<(
      std::ostream& os,
      const TreeMemoryTargetNode* node);

 private:
  int64_t freeBytes();

  int64_t borrow0(int64_t size);

  std::map<std::string, TreeMemoryTargetPtr> children_;
  TreeMemoryTargetWeakPtr parent_;
  std::string name_;
  int64_t capacity_;
  std::list<SpillerPtr> spillers_;
  std::map<std::string, MemoryUsageStatsBuilderPtr> virtualChildren_;
  SimpleMemoryUsageRecorder selfRecorder_;
  std::recursive_timed_mutex mutex_;
};

/* Bridge between Spark's MemoryConsumer and Gluten's MemoryTarget */

class ConsumerTargetBridge final
    : public MemoryConsumer,
      public TreeMemoryTarget,
      public std::enable_shared_from_this<ConsumerTargetBridge> {
 public:
  explicit ConsumerTargetBridge(TaskMemoryManagerWeakPtr taskMemoryManager);

  ~ConsumerTargetBridge() override;

  int64_t borrow(int64_t size) override;

  int64_t repay(int64_t size) override;

  const std::string& name() override;

  int64_t usedBytes() override;

  MemoryUsageStatsPtr stats() override;

  int64_t spill(int64_t size) override;

  int64_t acquireMemory(int64_t size) override;

  void freeMemory(int64_t size) override;

  TreeMemoryTargetPtr newChild(
      const std::string& name,
      const int64_t capacity,
      const std::list<SpillerPtr>& spillers,
      const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren)
      override;

  const std::map<std::string, TreeMemoryTargetPtr>& children() override;

  TreeMemoryTargetWeakPtr parent() override;

  const std::list<SpillerPtr>& getSpillers() override;

  void appendSpiller(SpillerPtr& spiller) override;

  TaskMemoryManagerWeakPtr getTaskMemoryManager();

  std::string toString() override;

  friend std::ostream& operator<<(
      std::ostream& os,
      const ConsumerTargetBridge& bridge);

  friend std::ostream& operator<<(
      std::ostream& os,
      const ConsumerTargetBridge* bridge);

 private:
  std::unique_ptr<SimpleMemoryUsageRecorder> recorder_;
  std::map<std::string, TreeMemoryTargetPtr> children_;
  std::string name_;
};

/* Some MemoryTarget being used in MemoryManagement */
class LoggingMemoryTarget final
    : public MemoryTarget,
      public std::enable_shared_from_this<LoggingMemoryTarget> {
 public:
  explicit LoggingMemoryTarget(TreeMemoryTargetPtr& delegated);

  ~LoggingMemoryTarget() override;

  int64_t borrow(int64_t size) override;

  int64_t repay(int64_t size) override;

  int64_t usedBytes() override;

  MemoryTargetPtr delegated();

  std::string toString() override;

  friend std::ostream& operator<<(
      std::ostream& os,
      const LoggingMemoryTarget& target);

  friend std::ostream& operator<<(
      std::ostream& os,
      const LoggingMemoryTarget* target);

 private:
  MemoryTargetPtr delegated_;
};

class OverAcquireMemoryTarget final
    : public MemoryTarget,
      public std::enable_shared_from_this<OverAcquireMemoryTarget> {
 public:
  OverAcquireMemoryTarget(
      MemoryTargetPtr& target,
      MemoryTargetPtr& overTarget,
      double ratio);

  ~OverAcquireMemoryTarget() override;

  int64_t borrow(int64_t size) override;

  int64_t repay(int64_t size) override;

  int64_t usedBytes() override;

  MemoryTargetPtr getTarget();

  std::string toString() override;

  friend std::ostream& operator<<(
      std::ostream& os,
      const OverAcquireMemoryTarget& target);

  friend std::ostream& operator<<(
      std::ostream& os,
      const OverAcquireMemoryTarget* target);

 private:
  MemoryTargetPtr target_, overTarget_;

  // The ratio is normally 0.
  //
  // If set to some value other than 0, the consumer will try
  //   over-acquire this ratio of memory each time it acquires
  //   from Spark.
  //
  // Once OOM, the over-acquired memory will be used as backup.
  //
  // The over-acquire is a general workaround for underling reservation
  //   procedures that were not perfectly-designed for spilling. For example,
  //   reservation for a two-step procedure: step A is capable for
  //   spilling while step B is not. If not reserving enough memory
  //   for step B before it's started, it might raise OOM since step A
  //   is ended and no longer open for spilling. In this case the
  //   over-acquired memory will be used in step B.
  double ratio_;
};

/* Factory to create ConsumerTargetBridge Node*/

class ConsumerTargetBridgeFactory final {
 public:
  explicit ConsumerTargetBridgeFactory(int64_t perTaskCapacity);

  ~ConsumerTargetBridgeFactory();

  TreeMemoryTargetPtr newConsumer(
      TaskMemoryManagerWeakPtr tmm,
      const std::string& name,
      const std::list<SpillerPtr>& spillers,
      const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren);

  void invalidate(TaskMemoryManagerWeakPtr tmm);

  std::string toString() const;

  friend std::ostream& operator<<(
      std::ostream& os,
      const ConsumerTargetBridgeFactory& factory);

  friend std::ostream& operator<<(
      std::ostream& os,
      const ConsumerTargetBridgeFactory* factory);

 private:
  TreeMemoryTargetPtr getSharedAccount(TaskMemoryManagerWeakPtr tmm);

  std::mutex mutex_;
  std::map<
      TaskMemoryManagerWeakPtr,
      TreeMemoryTargetPtr,
      TransparentWeakPtrComparator>
      rootNodeCache_;
  int64_t perTaskCapacity_;
};

/* SpillTrigger acts middle-layer between Spark's Consumer and Gluten's
 * MemoryTarget, receive spill signal from Spark's Consumer, then trigge
 * MemoryTarget's spill function */
class SpillTrigger final {
 public:
  constexpr static const std::array<SpillerPhase, 2> kSpillPhase{
      SpillerPhase::kShrink,
      SpillerPhase::kSpill};

  inline static const std::string kNamePrefix = "SpillTriggerLayer";

  static TreeMemoryTargetPtr newChild(
      TreeMemoryTargetWeakPtr parent,
      const std::string& name,
      const int64_t capacity,
      const std::list<SpillerPtr>& spillers,
      const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren);

  static int64_t spillTree(TreeMemoryTargetWeakPtr node, const int64_t bytes);

 private:
  SpillTrigger() = default;

  static int64_t spillTree(
      TreeMemoryTargetWeakPtr node,
      const int64_t bytes,
      const SpillerPhase& phase);
};

class MemoryTargetBuilder final {
 public:
  static MemoryTargetPtr overAcquire(
      MemoryTargetPtr& target,
      MemoryTargetPtr& overTarget,
      double overAcquiredRatio);

  static MemoryTargetPtr newConsumer(
      TaskMemoryManagerWeakPtr tmm,
      const std::string& name,
      const std::list<SpillerPtr>& spillers,
      const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren,
      const bool memoryIsolation,
      const int64_t conservativeTaskOffHeapMemorySize);

  static void invalidate(
      TaskMemoryManagerWeakPtr tmm,
      const bool memoryIsolation,
      const int64_t conservativeTaskOffHeapMemorySize);

 private:
  MemoryTargetBuilder() = default;

  static ConsumerTargetBridgeFactoryPtr isolated(
      const int64_t conservativeTaskOffHeapMemorySize);

  /**
   * This works as a legacy Spark memory consumer which grants as much as
   * possible of memory capacity to each task.
   */
  static ConsumerTargetBridgeFactoryPtr shared();

  static ConsumerTargetBridgeFactoryPtr createOrGetFactory(
      const int64_t perTaskCapacity);
};

/* Generate unique name for memory target */
class UniqueNameGenerator final {
 public:
  static std::string toUniqueName(const std::string& name);
};

} // namespace bytedance::bolt::memory::sparksql

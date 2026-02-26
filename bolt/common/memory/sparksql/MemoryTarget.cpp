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

#include <fmt/format.h>
#include <folly/system/ThreadName.h>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <utility>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/GlobalParameters.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/MemoryUsageStats.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/SpillerPhase.h"
#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
#include "bolt/common/memory/sparksql/WeakPtrHelper.h"
namespace bytedance::bolt::memory::sparksql {

TreeMemoryTargetNode::TreeMemoryTargetNode(
    TreeMemoryTargetWeakPtr parent,
    const std::string& name,
    const int64_t capacity,
    const std::list<SpillerPtr>& spillers,
    const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren)
    : parent_(parent),
      capacity_(capacity),
      spillers_(spillers),
      virtualChildren_(virtualChildren) {
  std::string uniqueName = UniqueNameGenerator::toUniqueName(name);
  if (capacity == kCapacityUnlimited) {
    name_ = uniqueName;
  } else {
    name_ = fmt::format("{}, {}", uniqueName, folly::to<std::string>(capacity));
  }
}

TreeMemoryTargetNode::~TreeMemoryTargetNode() {}

TreeMemoryTargetPtr TreeMemoryTargetNode::newChild(
    const std::string& name,
    const int64_t capacity,
    const std::list<SpillerPtr>& spillers,
    const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren) {
  TreeMemoryTargetNodePtr child = std::make_shared<TreeMemoryTargetNode>(
      weak_from_this(), name, capacity, spillers, virtualChildren);

  if (children_.find(child->name()) != children_.end()) {
    BOLT_FAIL("Child already registered: " + child->name());
  }
  children_.emplace(child->name(), child);
  return child;
}

int64_t TreeMemoryTargetNode::borrow(int64_t size) {
  if (isAsyncPreloadThread() && name_.find("root") != std::string::npos) {
    std::unique_lock<std::recursive_timed_mutex> lock(mutex_, std::defer_lock);
    if (!lock.try_lock_for(std::chrono::seconds(1))) {
      LOG(WARNING) << "Async preload thread can't lock busy "
                   << "TreeMemoryTargetNode " << name_ << " after 1s in thread "
                   << folly::getCurrentThreadName().value();
      return 0;
    }

    return borrow0(std::min(freeBytes(), size));
  }
  std::lock_guard<std::recursive_timed_mutex> lock(mutex_);
  return borrow0(std::min(freeBytes(), size));
}

const std::list<SpillerPtr>& TreeMemoryTargetNode::getSpillers() {
  return spillers_;
}

void TreeMemoryTargetNode::appendSpiller(SpillerPtr& spiller) {
  if (name().find(SpillTrigger::kNamePrefix) != std::string::npos) {
    spillers_.emplace_back(spiller);
    return;
  }
  auto parentShared = lock_or_throw(parent());
  parentShared->appendSpiller(spiller);
}

int64_t TreeMemoryTargetNode::repay(int64_t size) {
  std::lock_guard<std::recursive_timed_mutex> lock(mutex_);
  int64_t toFree = std::min(usedBytes(), size);
  auto parentShared = lock_or_throw(parent());
  int64_t freed = parentShared->repay(toFree);
  selfRecorder_.inc(-freed);
  return freed;
}

int64_t TreeMemoryTargetNode::usedBytes() {
  return selfRecorder_.current();
}

const std::string& TreeMemoryTargetNode::name() {
  return name_;
}

MemoryUsageStatsPtr TreeMemoryTargetNode::stats() {
  std::map<std::string, MemoryUsageStatsPtr> childrenStats;
  for (const auto& c : children_) {
    childrenStats.emplace(c.second->name(), c.second->stats());
  }
  BOLT_CHECK(childrenStats.size() == children_.size());

  // add virtual children
  for (const auto& entry : virtualChildren_) {
    if (childrenStats.find(entry.first) != childrenStats.end()) {
      BOLT_FAIL("Child stats already exists: " + entry.first);
    }
    childrenStats.emplace(entry.first, entry.second->toStats());
  }

  return selfRecorder_.toStats(childrenStats);
}

const std::map<std::string, TreeMemoryTargetPtr>&
TreeMemoryTargetNode::children() {
  return children_;
}

TreeMemoryTargetWeakPtr TreeMemoryTargetNode::parent() {
  return parent_;
}

std::string TreeMemoryTargetNode::toString() {
  std::stringstream ss;
  ss << this;
  return ss.str();
}

std::ostream& operator<<(std::ostream& os, const TreeMemoryTargetNode& node) {
  auto parentShared = node.parent_.lock();
  os << "TreeMemoryTargetNode(name=" << node.name_
     << ", capacity=" << node.capacity_
     << ", childrenNum=" << node.children_.size() << ", children=(";
  int64_t count = 0;
  for (const auto& child : node.children_) {
    os << "[seq=" << count++ << ", childName=" << child.first << "],";
  }
  os << "), parent="
     << (parentShared == nullptr ? "nullptr" : parentShared->toString());
  return os;
}

std::ostream& operator<<(std::ostream& os, const TreeMemoryTargetNode* node) {
  if (node) {
    return os << (*node);
  }
  return os << "TreeMemoryTargetNode(nullptr)";
}

int64_t TreeMemoryTargetNode::freeBytes() {
  return capacity_ - usedBytes();
}

int64_t TreeMemoryTargetNode::borrow0(int64_t size) {
  auto parentShared = lock_or_throw(parent());
  int64_t granted = parentShared->borrow(size);
  selfRecorder_.inc(granted);
  return granted;
}

/* ConsumerTargetBridge */
ConsumerTargetBridge::ConsumerTargetBridge(
    TaskMemoryManagerWeakPtr taskMemoryManager)
    : MemoryConsumer(taskMemoryManager), TreeMemoryTarget() {
  recorder_ = std::make_unique<SimpleMemoryUsageRecorder>();
  name_ = UniqueNameGenerator::toUniqueName("Gluten.Tree");
}

ConsumerTargetBridge::~ConsumerTargetBridge() {}

int64_t ConsumerTargetBridge::borrow(int64_t size) {
  if (size == 0) {
    // or Spark complains about the zero size by throwing an error
    return 0;
  }
  int64_t acquired = acquireMemory(size);
  recorder_->inc(acquired);
  return acquired;
}

int64_t ConsumerTargetBridge::repay(int64_t size) {
  if (size == 0) {
    return 0;
  }
  int64_t toFree = std::min(getUsed(), size);
  freeMemory(toFree);
  BOLT_CHECK(getUsed() >= 0);
  recorder_->inc(-toFree);
  return toFree;
}

const std::string& ConsumerTargetBridge::name() {
  return name_;
}

int64_t ConsumerTargetBridge::usedBytes() {
  return getUsed();
}

MemoryUsageStatsPtr ConsumerTargetBridge::stats() {
  std::map<std::string, MemoryUsageStatsPtr> childrenStats;
  for (const auto& pair : children_) {
    childrenStats.emplace(pair.second->name(), pair.second->stats());
  }

  BOLT_CHECK(childrenStats.size() == children_.size());
  MemoryUsageStatsPtr stats = recorder_->toStats(childrenStats);
  BOLT_CHECK(
      stats->current == getUsed(),
      "Used bytes mismatch between gluten memory consumer and Spark task memory manager");
  return stats;
}

int64_t ConsumerTargetBridge::spill(int64_t size) {
  // subject to the regular Spark spill calls
  return SpillTrigger::spillTree(weak_from_this(), size);
}

int64_t ConsumerTargetBridge::acquireMemory(int64_t size) {
  auto tmm = lock_or_throw(taskMemoryManager_);
  int64_t granted = tmm->acquireExecutionMemory(size, weak_from_this());
  used_ += granted;
  return granted;
}

void ConsumerTargetBridge::freeMemory(int64_t size) {
  BOLT_CHECK(size <= used_);
  auto tmm = lock_or_throw(taskMemoryManager_);
  int64_t released = tmm->releaseExecutionMemory(size, weak_from_this());
  used_ -= released;
}

TreeMemoryTargetPtr ConsumerTargetBridge::newChild(
    const std::string& name,
    const int64_t capacity,
    const std::list<SpillerPtr>& spillers,
    const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren) {
  TreeMemoryTargetPtr spillChild = SpillTrigger::newChild(
      weak_from_this(), name, capacity, spillers, virtualChildren);

  auto it = children_.find(spillChild->name());
  if (it != children_.end()) {
    BOLT_FAIL("Child already registered: {}", spillChild->name());
  }
  children_.emplace(spillChild->name(), spillChild);
  return spillChild;
}

const std::map<std::string, TreeMemoryTargetPtr>&
ConsumerTargetBridge::children() {
  return children_;
}

TreeMemoryTargetWeakPtr ConsumerTargetBridge::parent() {
  BOLT_FAIL("we are root, doesn't have parent node");
}

const std::list<SpillerPtr>& ConsumerTargetBridge::getSpillers() {
  // root doesn't spill
  static std::list<SpillerPtr> empty;
  // it's safe to return a static variable's reference
  return empty;
}

void ConsumerTargetBridge::appendSpiller(SpillerPtr& spiller) {
  BOLT_NYI("ConsumerTargetBridge doesn't have spillers");
}

TaskMemoryManagerWeakPtr ConsumerTargetBridge::getTaskMemoryManager() {
  return taskMemoryManager_;
}

std::string ConsumerTargetBridge::toString() {
  std::stringstream ss;
  ss << this;
  return ss.str();
}

std::ostream& operator<<(std::ostream& os, const ConsumerTargetBridge& bridge) {
  auto tmm = bridge.taskMemoryManager_.lock();
  os << "ConsumerTargetBridge(name=" << bridge.name_
     << ", taskMemoryManager=" << (tmm == nullptr ? "nullptr" : tmm->toString())
     << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ConsumerTargetBridge* bridge) {
  if (bridge) {
    return os << (*bridge);
  }
  return os << "ConsumerTargetBridge(nullptr)";
}

/* LoggingMemoryTarget */
LoggingMemoryTarget::LoggingMemoryTarget(TreeMemoryTargetPtr& delegated) {
  delegated_ = delegated;
}

LoggingMemoryTarget::~LoggingMemoryTarget() {}

int64_t LoggingMemoryTarget::borrow(int64_t size) {
  int64_t before = usedBytes();
  int64_t reserved = delegated_->borrow(size);
  int64_t after = usedBytes();
  LOG(INFO) << "In borrow function, before=" << before
            << ", reserved=" << reserved << ", size=" << size
            << ", after=" << after;
  return reserved;
}

int64_t LoggingMemoryTarget::repay(int64_t size) {
  int64_t before = usedBytes();
  int64_t unreserved = delegated_->repay(size);
  int64_t after = usedBytes();
  LOG(INFO) << "In repay function, before=" << before
            << ", unreserved=" << unreserved << ", size=" << size
            << ", after=" << after;
  return unreserved;
}

int64_t LoggingMemoryTarget::usedBytes() {
  return delegated_->usedBytes();
}

MemoryTargetPtr LoggingMemoryTarget::delegated() {
  return delegated_;
}

std::string LoggingMemoryTarget::toString() {
  std::stringstream ss;
  ss << this;
  return ss.str();
}

std::ostream& operator<<(std::ostream& os, const LoggingMemoryTarget& target) {
  os << "LoggingMemoryTarget(delegated=" << target.delegated_->toString()
     << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const LoggingMemoryTarget* target) {
  if (target) {
    return os << (*target);
  }
  return os << "LoggingMemoryTarget(nullptr)";
}

/* OverAcquireMemoryTarget */
OverAcquireMemoryTarget::OverAcquireMemoryTarget(
    MemoryTargetPtr& target,
    MemoryTargetPtr& overTarget,
    double ratio) {
  BOLT_CHECK(ratio >= 0.0);
  target_ = target;
  overTarget_ = overTarget;
  ratio_ = ratio;
}

OverAcquireMemoryTarget::~OverAcquireMemoryTarget() {}

int64_t OverAcquireMemoryTarget::borrow(int64_t size) {
  BOLT_CHECK(size != 0, "Size to borrow is zero");

  int64_t granted = target_->borrow(size);
  int64_t majorSize = target_->usedBytes();
  auto expectedOverAcquired = static_cast<int64_t>(ratio_ * majorSize);
  int64_t overAcquired = overTarget_->usedBytes();
  int64_t diff = expectedOverAcquired - overAcquired;
  if (diff >= 0) { // otherwise, there might be a spill happened during the
                   // last borrow() call
    try {
      overTarget_->borrow(diff); // we don't have to check the returned value
    } catch (const BoltException& e) {
      BOLT_FAIL(
          "Over-acquire memory target borrow failed, granted {}, try borrowing {}, current {}, ratio {}, error {}",
          granted,
          expectedOverAcquired,
          overAcquired,
          ratio_,
          e.message());
    }
  }
  return granted;
}

int64_t OverAcquireMemoryTarget::repay(int64_t size) {
  BOLT_CHECK(size != 0, "Size to repay is zero");
  int64_t freed = target_->repay(size);
  // clean up the over-acquired target
  int64_t overAcquired = overTarget_->usedBytes();
  int64_t freedOverAcquired = overTarget_->repay(overAcquired);

  BOLT_CHECK(
      freedOverAcquired == overAcquired,
      "Freed over-acquired size is not equal to requested size");
  BOLT_CHECK(
      overTarget_->usedBytes() == 0, "Over-acquired target was not cleaned up");

  return freed;
}

int64_t OverAcquireMemoryTarget::usedBytes() {
  return target_->usedBytes() + overTarget_->usedBytes();
}

MemoryTargetPtr OverAcquireMemoryTarget::getTarget() {
  return target_;
}

std::string OverAcquireMemoryTarget::toString() {
  std::stringstream ss;
  ss << this;
  return ss.str();
}

std::ostream& operator<<(
    std::ostream& os,
    const OverAcquireMemoryTarget& target) {
  os << "OverAcquireMemoryTarget(target=" << target.target_->toString()
     << ", overTarget=" << target.overTarget_->toString() << ")";
  return os;
}

std::ostream& operator<<(
    std::ostream& os,
    const OverAcquireMemoryTarget* target) {
  if (target) {
    return os << (*target);
  }
  return os << "OverAcquireMemoryTarget(nullptr)";
}

/* ConsumerTargetBridgeFactory */
ConsumerTargetBridgeFactory::ConsumerTargetBridgeFactory(
    int64_t perTaskCapacity)
    : perTaskCapacity_(perTaskCapacity) {}

ConsumerTargetBridgeFactory::~ConsumerTargetBridgeFactory() {}

TreeMemoryTargetPtr ConsumerTargetBridgeFactory::newConsumer(
    TaskMemoryManagerWeakPtr tmm,
    const std::string& name,
    const std::list<SpillerPtr>& spillers,
    const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren) {
  TreeMemoryTargetPtr account = getSharedAccount(tmm);
  return account->newChild(
      name,
      ConsumerTargetBridge::kCapacityUnlimited,
      spillers,
      virtualChildren);
}

void ConsumerTargetBridgeFactory::invalidate(TaskMemoryManagerWeakPtr tmm) {
  std::lock_guard<std::mutex> guard(mutex_);
  rootNodeCache_.erase(tmm);
}

std::string ConsumerTargetBridgeFactory::toString() const {
  std::stringstream ss;
  ss << this;
  return ss.str();
}

std::ostream& operator<<(
    std::ostream& os,
    const ConsumerTargetBridgeFactory& factory) {
  os << "ConsumerTargetBridgeFactory(perTaskCapacity_="
     << factory.perTaskCapacity_ << ", rootNodeCache_={";
  int count = 1;
  for (const auto& node : factory.rootNodeCache_) {
    auto tmm = node.first.lock();
    os << "seq=" << count << ", TaskMemoryManagerPtr=";
    if (tmm == nullptr) {
      os << "nullptr";
    } else {
      os << tmm;
    }
    os << ", TreeMemoryTargetPtr=" << node.second << "\n";
    count++;
  }
  os << "})";
  return os;
}

std::ostream& operator<<(
    std::ostream& os,
    const ConsumerTargetBridgeFactory* factory) {
  if (factory) {
    return os << (*factory);
  }
  return os << "ConsumerTargetBridgeFactory(nullptr)";
}

TreeMemoryTargetPtr ConsumerTargetBridgeFactory::getSharedAccount(
    TaskMemoryManagerWeakPtr tmm) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = rootNodeCache_.find(tmm);
  if (it == rootNodeCache_.end()) {
    ConsumerTargetBridgePtr bridge =
        std::make_shared<ConsumerTargetBridge>(tmm);

    // we need addConsumer manually to avoid bridge being destroyed
    MemoryConsumerPtr bridgeConsumer =
        std::dynamic_pointer_cast<MemoryConsumer>(bridge);
    BOLT_CHECK(bridgeConsumer != nullptr);

    auto tmmShared = lock_or_throw(tmm);
    tmmShared->registerConsumer(bridgeConsumer);

    TreeMemoryTargetPtr bridgeTree =
        std::dynamic_pointer_cast<TreeMemoryTarget>(bridge);
    BOLT_CHECK(bridgeTree != nullptr);
    auto child = bridgeTree->newChild("root", perTaskCapacity_, {}, {});
    rootNodeCache_.emplace(tmm, child);
    return child;
  }
  return it->second;
}

/* SpillTrigger */
TreeMemoryTargetPtr SpillTrigger::newChild(
    TreeMemoryTargetWeakPtr parent,
    const std::string& name,
    const int64_t capacity,
    const std::list<SpillerPtr>& spillers,
    const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren) {
  auto node = std::make_shared<TreeMemoryTargetNode>(
      parent,
      SpillTrigger::kNamePrefix + "." + name,
      capacity,
      spillers,
      virtualChildren);
  return node;
}

int64_t SpillTrigger::spillTree(
    TreeMemoryTargetWeakPtr node,
    const int64_t bytes) {
  // return if async IO prefetch threads trigger spill
  if (isAsyncPreloadThread()) {
    LOG(WARNING) << "Async preload thread can't trigger spill in thread "
                 << folly::getCurrentThreadName().value();
    return 0;
  }

  int64_t remainingBytes = bytes;
  for (const auto& phase : kSpillPhase) {
    // First shrink, then if no good, spill.
    if (remainingBytes <= 0) {
      break;
    }
    remainingBytes -= spillTree(node, remainingBytes, phase);
  }
  return bytes - remainingBytes;
}

int64_t SpillTrigger::spillTree(
    TreeMemoryTargetWeakPtr node,
    const int64_t bytes,
    const SpillerPhase& phase) {
  // sort children by used bytes, descending
  auto targetCompare = [](const TreeMemoryTargetPtr& o1,
                          const TreeMemoryTargetPtr& o2) {
    return o1->usedBytes() < o2->usedBytes();
  };
  std::priority_queue<
      TreeMemoryTargetPtr,
      std::vector<TreeMemoryTargetPtr>,
      decltype(targetCompare)>
      q(targetCompare);

  auto nodeShared = lock_or_throw(node);
  for (const auto& c : nodeShared->children()) {
    q.emplace(c.second);
  }

  int64_t remainingBytes = bytes;
  while (!q.empty() && q.top() != nullptr && remainingBytes > 0) {
    auto head = q.top();
    q.pop();
    int64_t spilled = spillTree(head, remainingBytes);
    remainingBytes -= spilled;
  }

  if (remainingBytes > 0) {
    // if still doesn't fit, spill self
    std::vector<SpillerPtr> applicableSpillers;
    for (const auto& spiller : nodeShared->getSpillers()) {
      auto applicablePhases = spiller->applicablePhases();
      if (applicablePhases.find(phase) != applicablePhases.end()) {
        applicableSpillers.emplace_back(spiller);
      }
    }

    for (int i = 0; i < applicableSpillers.size() && remainingBytes > 0; i++) {
      const auto& spiller = applicableSpillers.at(i);
      int64_t spilled = spiller->spill(node, remainingBytes);
      remainingBytes -= spilled;
    }
  }

  return bytes - remainingBytes;
}

/* MemoryTargetBuilder */
MemoryTargetPtr MemoryTargetBuilder::overAcquire(
    MemoryTargetPtr& target,
    MemoryTargetPtr& overTarget,
    double overAcquiredRatio) {
  if (overAcquiredRatio == 0.0) {
    return target;
  }
  return std::make_shared<OverAcquireMemoryTarget>(
      target, overTarget, overAcquiredRatio);
}

MemoryTargetPtr MemoryTargetBuilder::newConsumer(
    TaskMemoryManagerWeakPtr tmm,
    const std::string& name,
    const std::list<SpillerPtr>& spillers,
    const std::map<std::string, MemoryUsageStatsBuilderPtr>& virtualChildren,
    const bool memoryIsolation,
    const int64_t conservativeTaskOffHeapMemorySize) {
  ConsumerTargetBridgeFactoryPtr factory;
  if (memoryIsolation) {
    factory = isolated(conservativeTaskOffHeapMemorySize);
  } else {
    factory = shared();
  }
  return factory->newConsumer(tmm, name, spillers, virtualChildren);
}

void MemoryTargetBuilder::invalidate(
    TaskMemoryManagerWeakPtr tmm,
    const bool memoryIsolation,
    const int64_t conservativeTaskOffHeapMemorySize) {
  ConsumerTargetBridgeFactoryPtr factory;
  if (memoryIsolation) {
    factory = isolated(conservativeTaskOffHeapMemorySize);
  } else {
    factory = shared();
  }
  factory->invalidate(tmm);
}

ConsumerTargetBridgeFactoryPtr MemoryTargetBuilder::isolated(
    const int64_t conservativeTaskOffHeapMemorySize) {
  return createOrGetFactory(conservativeTaskOffHeapMemorySize);
}

/**
 * This works as a legacy Spark memory consumer which grants as much as
 * possible of memory capacity to each task.
 */
ConsumerTargetBridgeFactoryPtr MemoryTargetBuilder::shared() {
  return createOrGetFactory(TreeMemoryTarget::kCapacityUnlimited);
}

ConsumerTargetBridgeFactoryPtr MemoryTargetBuilder::createOrGetFactory(
    const int64_t perTaskCapacity) {
  static std::mutex lock;
  static std::map<int64_t, ConsumerTargetBridgeFactoryPtr> factoryCache;

  std::lock_guard<std::mutex> guard(lock);
  auto it = factoryCache.find(perTaskCapacity);
  if (it == factoryCache.end()) {
    auto ans = std::make_shared<ConsumerTargetBridgeFactory>(perTaskCapacity);
    factoryCache.emplace(perTaskCapacity, ans);
    return ans;
  }
  return it->second;
}

/* UniqueNameGenerator */
std::string UniqueNameGenerator::toUniqueName(const std::string& name) {
  static std::mutex m;
  static std::map<std::string, int64_t> uniqueNameFinder;

  std::lock_guard<std::mutex> guard(m);
  const auto& it = uniqueNameFinder.find(name);
  if (it == uniqueNameFinder.end()) {
    uniqueNameFinder.emplace(name, 0L);
    return fmt::format("{}.{}", name, 0);
  }
  auto alreadyUsedId = it->second;
  auto newId = alreadyUsedId + 1;
  // store newId as alreadyUsedId for next epoch
  uniqueNameFinder[name] = newId;
  return fmt::format("{}.{}", name, newId);
}

} // namespace bytedance::bolt::memory::sparksql

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

#include "bolt/exec/LocalPlanner.h"
#include "RoundRobinPartitionFunction.h"
#include "bolt/common/process/ExceptionTraceContext.h"
#include "bolt/core/PlanFragment.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/ArrowStream.h"
#include "bolt/exec/AssignUniqueId.h"
#include "bolt/exec/CallbackSink.h"
#include "bolt/exec/EnforceSingleRow.h"
#include "bolt/exec/Exchange.h"
#include "bolt/exec/Expand.h"
#include "bolt/exec/FilterProject.h"
#include "bolt/exec/Generator.h"
#include "bolt/exec/GroupId.h"
#include "bolt/exec/HashAggregation.h"
#include "bolt/exec/HashBuild.h"
#include "bolt/exec/HashProbe.h"
#include "bolt/exec/IndexLookupJoin.h"
#include "bolt/exec/Limit.h"
#include "bolt/exec/LocalShuffle.h"
#include "bolt/exec/MarkDistinct.h"
#include "bolt/exec/Merge.h"
#include "bolt/exec/MergeJoin.h"
#include "bolt/exec/NestedLoopJoinBuild.h"
#include "bolt/exec/NestedLoopJoinProbe.h"
#include "bolt/exec/OperatorTraceScan.h"
#include "bolt/exec/OrderBy.h"
#include "bolt/exec/PartitionedOutput.h"
#include "bolt/exec/RowNumber.h"
#include "bolt/exec/StreamingAggregation.h"
#include "bolt/exec/TableScan.h"
#include "bolt/exec/TableWriteMerge.h"
#include "bolt/exec/TableWriter.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/TopN.h"
#include "bolt/exec/TopNRowNumber.h"
#include "bolt/exec/Unnest.h"
#include "bolt/exec/Values.h"
#include "bolt/exec/Window.h"
#if defined BOLT_HAS_TORCH && BOLT_HAS_TORCH == 1
#include "bolt/torch/Operator.h"
#include "bolt/torch/PlanNode.h"
#endif
#if defined BOLT_HAS_PYTHON && BOLT_HAS_PYTHON == 1
#include "bolt/python/Operator.h"
#include "bolt/python/PlanNode.h"
#endif
#if defined BOLT_HAS_CUDF && BOLT_HAS_CUDF == 1
#include "bolt/cudf/CudfOperators.h"
#endif
namespace bytedance::bolt::exec {
namespace detail {

/// Returns true if source nodes must run in a separate pipeline.
bool mustStartNewPipeline(
    std::shared_ptr<const core::PlanNode> planNode,
    int sourceId) {
  if (auto localMerge =
          std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    // LocalMerge's source runs on its own pipeline.
    return true;
  }

  if (std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    return true;
  }

  // Non-first sources always run in their own pipeline.
  return sourceId != 0;
}

bool isIndexLookupJoin(core::PlanNodePtr planNode) {
  const auto indexLookupJoin =
      std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(planNode);
  return indexLookupJoin != nullptr;
}

OperatorSupplier makeOperatorSupplier(ConsumerSupplier consumerSupplier) {
  if (consumerSupplier) {
    return [consumerSupplier](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<CallbackSink>(
          operatorId, ctx, consumerSupplier());
    };
  }
  return nullptr;
}

OperatorSupplier makeOperatorSupplier(
    const std::shared_ptr<const core::PlanNode>& planNode) {
  if (auto localMerge =
          std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    return [localMerge](int32_t operatorId, DriverCtx* ctx) {
      auto mergeSource = ctx->task->addLocalMergeSource(
          ctx->splitGroupId,
          localMerge->id(),
          localMerge->outputType(),
          ctx->queryConfig().localMergeSourceQueueSize());

      auto consumerCb = [mergeSource](
                            RowVectorPtr input,
                            ContinueFuture* future,
                            uint32_t partitionId) {
        return mergeSource->enqueue(input, future);
      };
      auto startCb = [mergeSource](ContinueFuture* future) {
        return mergeSource->started(future);
      };
      return std::make_unique<CallbackSink>(
          operatorId, ctx, std::move(consumerCb), std::move(startCb));
    };
  }

  if (auto localPartitionNode =
          std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    return [localPartitionNode](int32_t operatorId, DriverCtx* ctx) {
      auto numPartitions = localPartitionNode->numPartitions();
      if (numPartitions > 0) {
        return std::make_unique<LocalPartition>(
            operatorId, numPartitions, ctx, localPartitionNode);
      }
      return std::make_unique<LocalPartition>(
          operatorId, ctx, localPartitionNode);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<HashBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<NestedLoopJoinBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
    auto planNodeId = planNode->id();
    return [planNodeId](int32_t operatorId, DriverCtx* ctx) {
      auto source =
          ctx->task->getMergeJoinSource(ctx->splitGroupId, planNodeId);
      auto consumer = [source](
                          RowVectorPtr input,
                          ContinueFuture* future,
                          uint32_t partitionId) {
        return source->enqueue(input, future);
      };
      return std::make_unique<CallbackSink>(operatorId, ctx, consumer);
    };
  }

  return Operator::operatorSupplierFromPlanNode(planNode);
}

const std::string PLAN_REWRITE_SUFFIX = ".rewrite";

void plan(
    const std::shared_ptr<const core::PlanNode>& planNode,
    std::vector<std::shared_ptr<const core::PlanNode>>* currentPlanNodes,
    const std::shared_ptr<const core::PlanNode>& consumerNode,
    OperatorSupplier operatorSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    const bool morselDriven) {
  if (!currentPlanNodes) {
    driverFactories->push_back(std::make_unique<DriverFactory>());
    currentPlanNodes = &driverFactories->back()->planNodes;
    driverFactories->back()->operatorSupplier = std::move(operatorSupplier);
    driverFactories->back()->consumerNode = consumerNode;
    // [morsel-driven] Pass the "morsel driven enabled" switch to driver factory
    if (morselDriven) {
      driverFactories->back()->enableMorselDriven();
    }
  }

  auto sources = planNode->sources();
  if (sources.empty()) {
    driverFactories->back()->inputDriver = true;
  } else {
    const auto numSourcesToPlan =
        isIndexLookupJoin(planNode) ? 1 : sources.size();
    for (int32_t i = 0; i < numSourcesToPlan; ++i) {
      // [morsel-driven] This is to split plan "Exchange->HashJoin(probe)" into
      // two pipelines, "Exchange->LocalPartition" and
      // "LocalExchange->HashJoin(probe)", in order to increase the impact of
      // morsel-driven to such plan segments.
      if (morselDriven && i == 0 &&
          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode) &&
          std::dynamic_pointer_cast<const core::ExchangeNode>(sources[i])) {
        // Create a LocalPartitionNode and bind it with current source node
        std::vector<core::PlanNodePtr> balancedSources{sources[i]};
        auto localPartitionNodeAdded =
            std::make_shared<core::LocalPartitionNode>(
                planNode->id() + PLAN_REWRITE_SUFFIX,
                core::LocalPartitionNode::Type::kRepartition,
                std::make_shared<RoundRobinPartitionFunctionSpec>(),
                balancedSources);

        // Replace the source with new LocalPartitionNode
        if (auto cHashJoinNode =
                std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
          if (auto hashJoinNode =
                  const_cast<core::HashJoinNode*>(cHashJoinNode.get())) {
            hashJoinNode->setSource(i, localPartitionNodeAdded);
          }
        }
        // Only has one source Exchange, keep creating plan between
        // LocalPartitionNode and Exchange Node.
        plan(
            localPartitionNodeAdded,
            mustStartNewPipeline(planNode, 0) ? nullptr : currentPlanNodes,
            planNode,
            makeOperatorSupplier(planNode),
            driverFactories,
            morselDriven);
      } else {
        plan(
            sources[i],
            mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes,
            planNode,
            makeOperatorSupplier(planNode),
            driverFactories,
            morselDriven);
      }
    }
  }

  currentPlanNodes->push_back(planNode);
}

// [morsel] Revert any plan rewrite under morsel-driven mode
//  before:
//           hashjoin
//          /         \
//   LocalPartition     other source
//      /
//    Exchange
//
//  after:
//           hashjoin
//          /         \
//   Exchange        other source
void revertPlanRewrite(const std::shared_ptr<const core::PlanNode>& planNode) {
  if (!planNode->sources().empty()) {
    int32_t sourceId = 0;
    for (auto& source : planNode->sources()) {
      if (source->id().ends_with(PLAN_REWRITE_SUFFIX) &&
          std::dynamic_pointer_cast<const core::LocalPartitionNode>(source)) {
        BOLT_CHECK(!source->sources().empty());
        if (auto cHashJoinNode =
                std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
          if (auto hashJoinNode =
                  const_cast<core::HashJoinNode*>(cHashJoinNode.get())) {
            hashJoinNode->setSource(sourceId, source->sources()[0]);
          }
        }
      }
      revertPlanRewrite(source);
      sourceId++;
    }
  }
}

// Sometimes consumer limits the number of drivers its producer can run.
uint32_t maxDriversForConsumer(
    const std::shared_ptr<const core::PlanNode>& node) {
  if (std::dynamic_pointer_cast<const core::MergeJoinNode>(node)) {
    // MergeJoinNode must run single-threaded.
    return 1;
  }
  return std::numeric_limits<uint32_t>::max();
}

uint32_t maxDrivers(
    const DriverFactory& driverFactory,
    const core::QueryConfig& queryConfig) {
  uint32_t count = maxDriversForConsumer(driverFactory.consumerNode);
  if (count == 1) {
    return count;
  }
  for (auto& node : driverFactory.planNodes) {
    if (auto topN = std::dynamic_pointer_cast<const core::TopNNode>(node)) {
      if (!topN->isPartial()) {
        // final topN must run single-threaded
        return 1;
      }
    } else if (
        auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
      // values node must run single-threaded, unless in test context
      if (!values->isParallelizable()) {
        return 1;
      }
    } else if (std::dynamic_pointer_cast<const core::ArrowStreamNode>(node)) {
      // ArrowStream node must run single-threaded.
      return 1;
    } else if (
        auto limit = std::dynamic_pointer_cast<const core::LimitNode>(node)) {
      // final limit must run single-threaded
      if (!limit->isPartial()) {
        return 1;
      }
    } else if (
        auto orderBy =
            std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
      // final orderby must run single-threaded
      if (!orderBy->isPartial()) {
        return 1;
      }
    } else if (
        auto localExchange =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(node)) {
      // Local gather must run single-threaded.
      if (localExchange->type() == core::LocalPartitionNode::Type::kGather) {
        return 1;
      }
    } else if (std::dynamic_pointer_cast<const core::LocalMergeNode>(node)) {
      // Local merge must run single-threaded.
      return 1;
    } else if (std::dynamic_pointer_cast<const core::MergeExchangeNode>(node)) {
      // Merge exchange must run single-threaded.
      return 1;
    } else if (std::dynamic_pointer_cast<const core::MergeJoinNode>(node)) {
      // Merge join must run single-threaded.
      return 1;
    } else if (
        auto tableWrite =
            std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      const auto& connectorInsertHandle =
          tableWrite->insertTableHandle()->connectorInsertTableHandle();
      if (!connectorInsertHandle->supportsMultiThreading()) {
        return 1;
      } else {
        if (tableWrite->hasPartitioningScheme()) {
          return queryConfig.taskPartitionedWriterCount();
        } else {
          return queryConfig.taskWriterCount();
        }
      }
    }
    // multi-threaded spark: ValueStream is designed to be single-threaded for
    // now. This assumption might not hold in the future.
    else if (node->name() == "ValueStream") {
      return 1;
    }
    // multi-threaded spark: SparkShuffleReader is designed to be
    // single-threaded for now. This assumption might not hold in the future.
    else if (node->name() == "SparkShuffleReader") {
      return 1;
    }
    // multi-threaded spark: SparkShuffleWriter is designed to be
    // single-threaded for now. This assumption might not hold in the future.
    else if (node->name() == "SparkShuffleWriter") {
      return 1;
    } else {
      auto result = Operator::maxDrivers(node);
      if (result) {
        BOLT_CHECK_GT(
            *result,
            0,
            "maxDrivers must be greater than 0. Plan node: {}",
            node->toString())
        if (*result == 1) {
          return 1;
        }
        count = std::min(*result, count);
      }
    }
  }
  return count;
}
} // namespace detail

// static
void LocalPlanner::plan(
    const core::PlanFragment& planFragment,
    ConsumerSupplier consumerSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    const core::QueryConfig& queryConfig,
    uint32_t maxDrivers,
    const bool multiDriverOpen,
    const bool morselDriven) {
  for (auto& adapter : DriverFactory::adapters) {
    if (adapter.inspect) {
      adapter.inspect(planFragment);
    }
  }
  detail::plan(
      planFragment.planNode,
      nullptr,
      nullptr,
      detail::makeOperatorSupplier(consumerSupplier),
      driverFactories,
      morselDriven);

  // [morsel] revert plan rewrite if no resulting pipeline(factory) can be
  // morsel-driven under the morsel-driven mode
  if (morselDriven) {
    bool morselDrivenPipelineFound{false};
    for (auto& factory : *driverFactories) {
      morselDrivenPipelineFound |= factory->isMorselDriven(true);
    }
    if (!morselDrivenPipelineFound) {
      (*driverFactories).clear();
      detail::revertPlanRewrite(planFragment.planNode);
      detail::plan(
          planFragment.planNode,
          nullptr,
          nullptr,
          detail::makeOperatorSupplier(consumerSupplier),
          driverFactories,
          false);
    }
  }

  if (queryConfig.exceptionTraceLevel() != 1 ||
      queryConfig.exceptionTraceWhitelist().size() > 1) {
    process::ExceptionTraceContext::get_instance().set_level(
        queryConfig.exceptionTraceLevel());
    process::ExceptionTraceContext::get_instance().set_whitelist(
        queryConfig.exceptionTraceWhitelist());
  }

  (*driverFactories)[0]->outputDriver = true;

  if (planFragment.isGroupedExecution()) {
    determineGroupedExecutionPipelines(planFragment, *driverFactories);
    markMixedJoinBridges(*driverFactories);
  }

  // Determine number of drivers for each pipeline.
  for (auto& factory : *driverFactories) {
    factory->maxDrivers = detail::maxDrivers(*factory, queryConfig);
    factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);

#ifndef SPARK_COMPATIBLE
    bool flag = false;
    for (auto& node : factory->planNodes) {
      if (std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
        flag = true;
      }
    }
    if (flag && multiDriverOpen) {
      factory->numDrivers = std::min(factory->maxDrivers, maxDrivers * 10);
    } else {
      factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);
    }
#else
    factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);
#endif

    // Pipelines running grouped/bucketed execution would have separate groups
    // of drivers dealing with separate split groups (one driver can access
    // splits from only one designated split group), hence we will have total
    // number of drivers multiplied by the number of split groups.
    if (factory->groupedExecution) {
      factory->numTotalDrivers =
          factory->numDrivers * planFragment.numSplitGroups;
    } else {
      factory->numTotalDrivers = factory->numDrivers;
    }
  }
}

// static
void LocalPlanner::determineGroupedExecutionPipelines(
    const core::PlanFragment& planFragment,
    std::vector<std::unique_ptr<DriverFactory>>& driverFactories) {
  // We run backwards - from leaf pipelines to the root pipeline.
  for (auto it = driverFactories.rbegin(); it != driverFactories.rend(); ++it) {
    auto& factory = *it;

    // See if pipelines have leaf nodes that use grouped execution strategy.
    if (planFragment.leafNodeRunsGroupedExecution(factory->leafNodeId())) {
      factory->groupedExecution = true;
    }

    // If a pipeline's leaf node is Local Partition, which has all sources
    // belonging to pipelines that run Grouped Execution, then our pipeline
    // should run Grouped Execution as well.
    if (auto localPartitionNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                factory->planNodes.front())) {
      size_t numGroupedExecutionSources{0};
      for (const auto& sourceNode : localPartitionNode->sources()) {
        for (auto& anotherFactory : driverFactories) {
          if (sourceNode == anotherFactory->planNodes.back() and
              anotherFactory->groupedExecution) {
            ++numGroupedExecutionSources;
            break;
          }
        }
      }
      if (numGroupedExecutionSources > 0 and
          numGroupedExecutionSources == localPartitionNode->sources().size()) {
        factory->groupedExecution = true;
      }
    }
  }
}

// static
void LocalPlanner::markMixedJoinBridges(
    std::vector<std::unique_ptr<DriverFactory>>& driverFactories) {
  for (auto& factory : driverFactories) {
    // We are interested in grouped execution pipelines only.
    if (!factory->groupedExecution) {
      continue;
    }

    // See if we have any join nodes.
    for (const auto& planNode : factory->planNodes) {
      if (auto joinNode =
              std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
        // See if the build source (2nd) belongs to an ungrouped execution.
        auto& buildSourceNode = planNode->sources()[1];
        for (auto& factoryOther : driverFactories) {
          if (!factoryOther->groupedExecution &&
              buildSourceNode->id() == factoryOther->outputNodeId()) {
            factoryOther->mixedExecutionModeHashJoinNodeIds.emplace(
                planNode->id());
            factory->mixedExecutionModeHashJoinNodeIds.emplace(planNode->id());
            break;
          }
        }
      } else if (
          auto joinNode =
              std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                  planNode)) {
        // See if the build source (2nd) belongs to an ungrouped execution.
        auto& buildSourceNode = planNode->sources()[1];
        for (auto& factoryOther : driverFactories) {
          if (!factoryOther->groupedExecution &&
              buildSourceNode->id() == factoryOther->outputNodeId()) {
            factoryOther->mixedExecutionModeNestedLoopJoinNodeIds.emplace(
                planNode->id());
            factory->mixedExecutionModeNestedLoopJoinNodeIds.emplace(
                planNode->id());
            break;
          }
        }
      }
    }
  }
}

namespace {

// If the upstream is partial limit, downstream is final limit and we want to
// flush as soon as we can to reach the limit and do as little work as possible.
bool eagerFlush(const core::PlanNode& node) {
  if (auto* limit = dynamic_cast<const core::LimitNode*>(&node)) {
    return limit->isPartial() && limit->offset() + limit->count() < 10'000;
  }
  if (node.sources().empty()) {
    return false;
  }
  // Follow the first source, which is driving the output.
  return eagerFlush(*node.sources()[0]);
}

} // namespace

std::shared_ptr<Driver> DriverFactory::createDriver(
    std::unique_ptr<DriverCtx> ctx,
    std::shared_ptr<ExchangeClient> exchangeClient,
    std::function<int(int pipelineId)> numDrivers,
    std::shared_ptr<LocalExchangeQueue> primedQueue) {
  auto driver = std::shared_ptr<Driver>(new Driver());
  ctx->driver = driver.get();
  std::vector<std::unique_ptr<Operator>> operators;
  operators.reserve(planNodes.size());
  bool markSkipProject = false;
  int markSkipProjectAggIndex = planNodes.size() - 3;
  if (planNodes.size() > 0 &&
      planNodes.back()->name() == "SparkShuffleWriter") {
    // shufflewriter + project + project + agg
    markSkipProjectAggIndex = planNodes.size() - 4;
  }

  for (int32_t i = 0; i < planNodes.size(); i++) {
    // Id of the Operator being made. This is not the same as 'i'
    // because some PlanNodes may get fused.
    auto id = operators.size();
    auto planNode = planNodes[i];
    if (auto filterNode =
            std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
      if (i < planNodes.size() - 1) {
        auto next = planNodes[i + 1];
        if (auto projectNode =
                std::dynamic_pointer_cast<const core::ProjectNode>(next)) {
          operators.push_back(std::make_unique<FilterProject>(
              id, ctx.get(), filterNode, projectNode));
          i++;
          continue;
        }
      }
      operators.push_back(
          std::make_unique<FilterProject>(id, ctx.get(), filterNode, nullptr));
    }
#if defined BOLT_HAS_TORCH && BOLT_HAS_TORCH == 1
    else if (
        auto torchNode = std::dynamic_pointer_cast<
            const ::bytedance::bolt::torch::TorchNode>(planNode)) {
      auto op = std::make_unique<bytedance::bolt::torch::TorchOperator>(
          torchNode->moduleScript(),
          torchNode->outputType(),
          id,
          ctx.get(),
          torchNode->id());
      operators.push_back(std::move(op));
    }
#endif
#if defined BOLT_HAS_PYTHON && BOLT_HAS_PYTHON == 1
    else if (
        auto pythonNode =
            std::dynamic_pointer_cast<const ::bolt::python::PythonNode>(
                planNode)) {
      auto op = std::make_unique<bolt::python::PythonOperator>(
          pythonNode->functionName(),
          pythonNode->function(),
          pythonNode->args(),
          pythonNode->kwargs(),
          pythonNode->outputType(),
          id,
          ctx.get(),
          pythonNode->id());
      operators.push_back(std::move(op));
    }
#endif
    else if (
        auto projectNode =
            std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
#ifdef SPARK_COMPATIBLE
      // partialagg + project + project
      if (markSkipProject && (i = markSkipProjectAggIndex + 1)) {
        auto nextProject = std::dynamic_pointer_cast<const core::ProjectNode>(
            planNodes[markSkipProjectAggIndex + 2]);
        if (nextProject && nextProject->projections().size() > 0) {
          auto callTyped = std::dynamic_pointer_cast<const core::CallTypedExpr>(
              nextProject->projections()[0]);
          if (LIKELY(
                  callTyped &&
                  (callTyped->name() == "hive_hash" ||
                   callTyped->name() == "hash_with_seed"))) {
            operators.push_back(std::make_unique<FilterProject>(
                id,
                ctx.get(),
                nullptr,
                projectNode,
                FilterProject::ProjectType::kSkipCompositeRowVector));
            markSkipProject = false;
            LOG(INFO) << "mark last project as skip";
            continue;
          } else {
            LOG(INFO) << "next PlanNode is not hashFunction, callTyped = "
                      << callTyped << ", function name is "
                      << (callTyped ? callTyped->name() : "unknown");
          }
        } else {
          LOG(INFO) << "markSkipProject but next PlanNode is not ProjectNode";
        }
        markSkipProject = false;
      }

      // (valuestream or shuffle reader) + project + agg
      if (i == 1 &&
          (planNodes[0]->name() == "ValueStream" ||
           planNodes[0]->name() == "SparkShuffleReader") &&
          planNodes.size() >= 3 &&
          std::dynamic_pointer_cast<const core::AggregationNode>(
              planNodes[2])) {
        operators.push_back(std::make_unique<FilterProject>(
            id,
            ctx.get(),
            nullptr,
            projectNode,
            FilterProject::ProjectType::kSkipCompositeRowVector));
        continue;
      }
#endif
      operators.push_back(
          std::make_unique<FilterProject>(id, ctx.get(), nullptr, projectNode));
    } else if (
        auto valuesNode =
            std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
      operators.push_back(std::make_unique<Values>(id, ctx.get(), valuesNode));
    } else if (
        auto arrowStreamNode =
            std::dynamic_pointer_cast<const core::ArrowStreamNode>(planNode)) {
      operators.push_back(
          std::make_unique<ArrowStream>(id, ctx.get(), arrowStreamNode));
    } else if (
        auto tableScanNode =
            std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
      operators.push_back(
          std::make_unique<TableScan>(id, ctx.get(), tableScanNode));
    } else if (
        auto tableWriteNode =
            std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
      operators.push_back(
          std::make_unique<TableWriter>(id, ctx.get(), tableWriteNode));
    } else if (
        auto tableWriteMergeNode =
            std::dynamic_pointer_cast<const core::TableWriteMergeNode>(
                planNode)) {
      operators.push_back(std::make_unique<TableWriteMerge>(
          id, ctx.get(), tableWriteMergeNode));
    } else if (
        auto mergeExchangeNode =
            std::dynamic_pointer_cast<const core::MergeExchangeNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<MergeExchange>(i, ctx.get(), mergeExchangeNode));
    } else if (
        auto exchangeNode =
            std::dynamic_pointer_cast<const core::ExchangeNode>(planNode)) {
      // NOTE: the exchange client can only be used by one operator in a driver.
      BOLT_CHECK_NOT_NULL(exchangeClient);
      operators.push_back(std::make_unique<Exchange>(
          id, ctx.get(), exchangeNode, std::move(exchangeClient)));
    } else if (
        auto partitionedOutputNode =
            std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
                planNode)) {
      operators.push_back(std::make_unique<PartitionedOutput>(
          id, ctx.get(), partitionedOutputNode, eagerFlush(*planNode)));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      operators.push_back(std::make_unique<HashProbe>(id, ctx.get(), joinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<NestedLoopJoinProbe>(id, ctx.get(), joinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<IndexLookupJoin>(id, ctx.get(), joinNode));
    } else if (
        auto aggregationNode =
            std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
      if (!aggregationNode->preGroupedKeys().empty() &&
          aggregationNode->preGroupedKeys().size() ==
              aggregationNode->groupingKeys().size()) {
        operators.push_back(std::make_unique<StreamingAggregation>(
            id, ctx.get(), aggregationNode));
      } else {
#ifdef SPARK_COMPATIBLE
        // for function like avg/first, intermediate type is ROW<>, eg
        // ROW<double, bigint> for avg a ProjectNode is added by gluten to
        // project ROW<double, bigint> to 2 columns and row_contructor these 2
        // columns back to ROW after shuffle read for CompositeRowVector, skip
        // project and row_contructor execution
        markSkipProject =
            (aggregationNode->isPartial() && i == markSkipProjectAggIndex);
#endif
#if defined BOLT_HAS_CUDF && BOLT_HAS_CUDF == 1
        // Check if all aggregates are supported by cuDF Operator
        bool isCudfSupported =
            bolt::cudf::exec::HashAggregation::isSupported(aggregationNode);
        if (aggregationNode->useCudf() && isCudfSupported) {
          operators.push_back(
              std::make_unique<bolt::cudf::exec::HashAggregation>(
                  id, ctx.get(), aggregationNode));
        } else {
          if (aggregationNode->useCudf() && !isCudfSupported) {
            LOG(INFO) << "HashAggregation is not supported, fallback to CPU";
          }
          operators.push_back(std::make_unique<HashAggregation>(
              id, ctx.get(), aggregationNode));
        }
#else
        operators.push_back(
            std::make_unique<HashAggregation>(id, ctx.get(), aggregationNode));
#endif
      }
    } else if (
        auto expandNode =
            std::dynamic_pointer_cast<const core::ExpandNode>(planNode)) {
      operators.push_back(std::make_unique<Expand>(id, ctx.get(), expandNode));
    } else if (
        auto groupIdNode =
            std::dynamic_pointer_cast<const core::GroupIdNode>(planNode)) {
      operators.push_back(
          std::make_unique<GroupId>(id, ctx.get(), groupIdNode));
    } else if (
        auto topNNode =
            std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
      operators.push_back(std::make_unique<TopN>(id, ctx.get(), topNNode));
    } else if (
        auto limitNode =
            std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
      operators.push_back(std::make_unique<Limit>(id, ctx.get(), limitNode));
    } else if (
        auto orderByNode =
            std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
#if defined BOLT_HAS_CUDF && BOLT_HAS_CUDF == 1
      if (orderByNode->useCudf()) {
        operators.push_back(std::make_unique<bolt::cudf::exec::OrderBy>(
            id, ctx.get(), orderByNode));
      } else {
        operators.push_back(
            std::make_unique<OrderBy>(id, ctx.get(), orderByNode));
      }
#else
      operators.push_back(
          std::make_unique<OrderBy>(id, ctx.get(), orderByNode));
#endif
    } else if (
        auto windowNode =
            std::dynamic_pointer_cast<const core::WindowNode>(planNode)) {
      operators.push_back(std::make_unique<Window>(id, ctx.get(), windowNode));
    } else if (
        auto rowNumberNode =
            std::dynamic_pointer_cast<const core::RowNumberNode>(planNode)) {
      operators.push_back(
          std::make_unique<RowNumber>(id, ctx.get(), rowNumberNode));
    } else if (
        auto topNRowNumberNode =
            std::dynamic_pointer_cast<const core::TopNRowNumberNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<TopNRowNumber>(id, ctx.get(), topNRowNumberNode));
    } else if (
        auto markDistinctNode =
            std::dynamic_pointer_cast<const core::MarkDistinctNode>(planNode)) {
      operators.push_back(
          std::make_unique<MarkDistinct>(id, ctx.get(), markDistinctNode));
    } else if (
        auto localMerge =
            std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
      auto localMergeOp =
          std::make_unique<LocalMerge>(id, ctx.get(), localMerge);
      operators.push_back(std::move(localMergeOp));
    } else if (
        auto mergeJoin =
            std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
      auto mergeJoinOp = std::make_unique<MergeJoin>(id, ctx.get(), mergeJoin);
      ctx->task->createMergeJoinSource(ctx->splitGroupId, mergeJoin->id());
      operators.push_back(std::move(mergeJoinOp));
    } else if (
        auto localPartitionNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                planNode)) {
      operators.push_back(std::make_unique<LocalExchange>(
          id,
          ctx.get(),
          localPartitionNode->outputType(),
          localPartitionNode->id(),
          ctx->partitionId,
          primedQueue));
    } else if (
        auto unnest =
            std::dynamic_pointer_cast<const core::UnnestNode>(planNode)) {
      operators.push_back(std::make_unique<Unnest>(id, ctx.get(), unnest));
    } else if (
        auto generator =
            std::dynamic_pointer_cast<const core::GeneratorNode>(planNode)) {
      if (generator->isExplode()) {
        operators.push_back(
            std::make_unique<GeneratorExplode>(id, ctx.get(), generator));
      } else if (generator->isJsonTuple()) {
        operators.push_back(
            std::make_unique<GeneratorJsonTuple>(id, ctx.get(), generator));
      } else {
        BOLT_UNSUPPORTED("Generator support explode/json_tuple only");
      }
    } else if (
        auto enforceSingleRow =
            std::dynamic_pointer_cast<const core::EnforceSingleRowNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<EnforceSingleRow>(id, ctx.get(), enforceSingleRow));
    } else if (
        auto assignUniqueIdNode =
            std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(
                planNode)) {
      operators.push_back(std::make_unique<AssignUniqueId>(
          id,
          ctx.get(),
          assignUniqueIdNode,
          assignUniqueIdNode->taskUniqueId(),
          assignUniqueIdNode->uniqueIdCounter()));
    } else if (
        const auto traceScanNode =
            std::dynamic_pointer_cast<const core::TraceScanNode>(planNode)) {
      operators.push_back(std::make_unique<trace::OperatorTraceScan>(
          id, ctx.get(), traceScanNode));
    } else if (
        const auto localShuffleNode =
            std::dynamic_pointer_cast<const core::LocalShuffleNode>(planNode)) {
      operators.push_back(
          std::make_unique<LocalShuffle>(id, ctx.get(), localShuffleNode));
    } else {
      std::unique_ptr<Operator> extended;
      if (planNode->requiresExchangeClient()) {
        // NOTE: the exchange client can only be used by one operator in a
        // driver.
        BOLT_CHECK_NOT_NULL(exchangeClient);
        extended = Operator::fromPlanNode(
            ctx.get(), id, planNode, std::move(exchangeClient));
      } else {
        extended = Operator::fromPlanNode(ctx.get(), id, planNode);
      }
      BOLT_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
      operators.push_back(std::move(extended));
    }
  }
  if (operatorSupplier) {
    operators.push_back(operatorSupplier(operators.size(), ctx.get()));
  }

  driver->init(std::move(ctx), std::move(operators));
  for (auto& adapter : adapters) {
    if (adapter.adapt(*this, *driver)) {
      break;
    }
  }
  driver->isAdaptable_ = false;
  return driver;
} // namespace bytedance::bolt::exec

std::vector<std::unique_ptr<Operator>> DriverFactory::replaceOperators(
    Driver& driver,
    int32_t begin,
    int32_t end,
    std::vector<std::unique_ptr<Operator>> replaceWith) const {
  BOLT_CHECK(driver.isAdaptable_);
  std::vector<std::unique_ptr<exec::Operator>> replaced;
  for (auto i = begin; i < end; ++i) {
    replaced.push_back(std::move(driver.operators_[i]));
  }

  driver.operators_.erase(
      driver.operators_.begin() + begin, driver.operators_.begin() + end);

  // Insert the replacement at the place of the erase. Do manually because
  // insert() is not good with unique pointers.
  driver.operators_.resize(driver.operators_.size() + replaceWith.size());
  for (int32_t i = driver.operators_.size() - 1;
       i >= begin + replaceWith.size();
       --i) {
    driver.operators_[i] = std::move(driver.operators_[i - replaceWith.size()]);
  }
  for (auto i = 0; i < replaceWith.size(); ++i) {
    driver.operators_[i + begin] = std::move(replaceWith[i]);
  }

  // Set the ids to be consecutive.
  for (auto i = 0; i < driver.operators_.size(); ++i) {
    driver.operators_[i]->setOperatorIdFromAdapter(i);
  }
  return replaced;
}

std::vector<core::PlanNodeId> DriverFactory::needsHashJoinBridges() const {
  std::vector<core::PlanNodeId> planNodeIds;
  // Ungrouped execution pipelines need to take care of cross-mode bridges.
  if (!groupedExecution && !mixedExecutionModeHashJoinNodeIds.empty()) {
    planNodeIds.insert(
        planNodeIds.end(),
        mixedExecutionModeHashJoinNodeIds.begin(),
        mixedExecutionModeHashJoinNodeIds.end());
  }
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      if (!groupedExecution ||
          !mixedExecutionModeHashJoinNodeIds.contains(joinNode->id())) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
  }
  return planNodeIds;
}

std::vector<core::PlanNodeId> DriverFactory::needsNestedLoopJoinBridges()
    const {
  std::vector<core::PlanNodeId> planNodeIds;
  // Ungrouped execution pipelines need to take care of cross-mode bridges.
  if (!groupedExecution && !mixedExecutionModeNestedLoopJoinNodeIds.empty()) {
    planNodeIds.insert(
        planNodeIds.end(),
        mixedExecutionModeNestedLoopJoinNodeIds.begin(),
        mixedExecutionModeNestedLoopJoinNodeIds.end());
  }
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      if (!groupedExecution ||
          !mixedExecutionModeNestedLoopJoinNodeIds.contains(joinNode->id())) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
  }

  return planNodeIds;
}

// static
void DriverFactory::registerAdapter(DriverAdapter adapter) {
  adapters.push_back(std::move(adapter));
}

// static
std::vector<DriverAdapter> DriverFactory::adapters;

} // namespace bytedance::bolt::exec

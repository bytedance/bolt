/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <folly/init/Init.h>
#include <folly/json.h>
#include <gflags/gflags.h>

#include <chrono>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/TraceUtil.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"

using namespace bytedance::bolt;

namespace {

DEFINE_string(request, "", "Path to the JSON request payload.");
DEFINE_string(
    trace_root,
    "",
    "Directory where Bolt trace artifacts are written.");

TypePtr parseTypeName(const std::string& typeName) {
  if (typeName == "BIGINT") {
    return BIGINT();
  }
  if (typeName == "INTEGER") {
    return INTEGER();
  }
  if (typeName == "DOUBLE") {
    return DOUBLE();
  }
  if (typeName == "VARCHAR") {
    return VARCHAR();
  }
  if (typeName == "BOOLEAN") {
    return BOOLEAN();
  }
  BOLT_FAIL("Unsupported input type '{}'", typeName);
}

variant parseVariant(const folly::dynamic& value, const TypePtr& type) {
  if (value.isNull()) {
    return variant::null(type->kind());
  }

  switch (type->kind()) {
    case TypeKind::BIGINT:
      return variant(static_cast<int64_t>(value.asInt()));
    case TypeKind::INTEGER:
      return variant(static_cast<int32_t>(value.asInt()));
    case TypeKind::DOUBLE:
      return variant(value.asDouble());
    case TypeKind::VARCHAR:
      return variant(value.asString());
    case TypeKind::BOOLEAN:
      return variant(value.asBool());
    default:
      BOLT_FAIL("Unsupported scalar type '{}'", type->toString());
  }
}

VectorPtr makeFlatVectorFromVariants(
    memory::MemoryPool* pool,
    const TypePtr& type,
    const std::vector<variant>& values) {
  auto vector = BaseVector::create(type, values.size(), pool);
  for (vector_size_t row = 0; row < values.size(); ++row) {
    const auto& value = values[row];
    if (value.isNull()) {
      vector->setNull(row, true);
      continue;
    }
    switch (type->kind()) {
      case TypeKind::BIGINT:
        vector->asFlatVector<int64_t>()->set(row, value.value<int64_t>());
        break;
      case TypeKind::INTEGER:
        vector->asFlatVector<int32_t>()->set(row, value.value<int32_t>());
        break;
      case TypeKind::DOUBLE:
        vector->asFlatVector<double>()->set(row, value.value<double>());
        break;
      case TypeKind::VARCHAR:
        vector->asFlatVector<StringView>()->set(
            row, StringView(value.value<const char*>()));
        break;
      case TypeKind::BOOLEAN:
        vector->asFlatVector<bool>()->set(row, value.value<bool>());
        break;
      default:
        BOLT_FAIL("Unsupported scalar type '{}'", type->toString());
    }
  }
  return vector;
}

RowVectorPtr buildInputVector(
    memory::MemoryPool* pool,
    const folly::dynamic& inputObj) {
  const auto& schemaObj = inputObj["schema"];
  const auto& rowsObj = inputObj["rows"];

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<std::vector<variant>> columns;

  names.reserve(schemaObj.size());
  types.reserve(schemaObj.size());
  columns.resize(schemaObj.size());

  for (const auto& columnObj : schemaObj) {
    names.push_back(columnObj["name"].asString());
    types.push_back(parseTypeName(columnObj["type"].asString()));
  }

  for (const auto& rowObj : rowsObj) {
    BOLT_USER_CHECK_EQ(
        rowObj.size(),
        schemaObj.size(),
        "Row width {} doesn't match schema width {}",
        rowObj.size(),
        schemaObj.size());
    for (auto column = 0; column < rowObj.size(); ++column) {
      columns[column].push_back(parseVariant(rowObj[column], types[column]));
    }
  }

  std::vector<VectorPtr> children;
  children.reserve(types.size());
  for (auto column = 0; column < types.size(); ++column) {
    children.push_back(
        makeFlatVectorFromVariants(pool, types[column], columns[column]));
  }

  return std::make_shared<RowVector>(
      pool,
      ROW(std::move(names), std::move(types)),
      BufferPtr(nullptr),
      rowsObj.size(),
      std::move(children));
}

struct RequestOptions {
  std::string requestPath;
  std::string traceRoot;
};

RequestOptions parseArgs() {
  RequestOptions options{FLAGS_request, FLAGS_trace_root};
  BOLT_USER_CHECK(!options.requestPath.empty(), "--request is required");
  BOLT_USER_CHECK(!options.traceRoot.empty(), "--trace-root is required");
  return options;
}

std::string readFile(const std::string& path) {
  std::ifstream file(path);
  BOLT_USER_CHECK(file.good(), "Failed to open request file '{}'", path);
  std::ostringstream out;
  out << file.rdbuf();
  return out.str();
}

std::vector<std::string> collectTraceNodeIds(
    const std::vector<std::pair<std::string, std::string>>& tracedNodes) {
  std::vector<std::string> ids;
  ids.reserve(tracedNodes.size());
  for (const auto& tracedNode : tracedNodes) {
    ids.push_back(tracedNode.first);
  }
  return ids;
}

std::string joinComma(const std::vector<std::string>& values) {
  std::ostringstream out;
  for (auto i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out << ",";
    }
    out << values[i];
  }
  return out.str();
}

folly::dynamic tracedNodesJson(
    const std::vector<std::pair<std::string, std::string>>& tracedNodes) {
  folly::dynamic array = folly::dynamic::array;
  for (const auto& [planNodeId, nodeName] : tracedNodes) {
    folly::dynamic item = folly::dynamic::object;
    item["plan_node_id"] = planNodeId;
    item["node_name"] = nodeName;
    array.push_back(item);
  }
  return array;
}

folly::dynamic rowsToJson(
    const RowVectorPtr& rowVector,
    size_t rowLimit = 100) {
  folly::dynamic rows = folly::dynamic::array;
  const auto end = std::min<size_t>(rowVector->size(), rowLimit);
  for (size_t row = 0; row < end; ++row) {
    rows.push_back(rowVector->toString(static_cast<vector_size_t>(row)));
  }
  return rows;
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  const auto options = parseArgs();

  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  filesystems::registerLocalFileSystem();

  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  auto pool = memory::memoryManager()->addLeafPool();

  const auto request = folly::parseJson(readFile(options.requestPath));
  const auto queryId =
      request.getDefault("query_id", "bolt_agent_query").asString();
  const auto repeatTimes = request.getDefault("repeat_times", 1).asInt();
  const auto maxDrivers = request.getDefault("max_drivers", 1).asInt();

  auto input = buildInputVector(pool.get(), request["input"]);
  exec::test::PlanBuilder builder(pool.get());
  std::vector<std::pair<std::string, std::string>> tracedNodes;

  builder.values({input}, false, repeatTimes);

  const auto& planObj = request["plan"];
  if (planObj.count("filter") > 0 && !planObj["filter"].asString().empty()) {
    core::PlanNodeId nodeId;
    builder.filter(planObj["filter"].asString()).capturePlanNodeId(nodeId);
    tracedNodes.push_back({nodeId, "FilterProject"});
  }

  if (planObj.count("project") > 0 && !planObj["project"].empty()) {
    std::vector<std::string> projections;
    for (const auto& projection : planObj["project"]) {
      projections.push_back(projection.asString());
    }
    core::PlanNodeId nodeId;
    builder.project(projections).capturePlanNodeId(nodeId);
    tracedNodes.push_back({nodeId, "FilterProject"});
  }

  if (planObj.count("aggregation") > 0) {
    const auto& aggregation = planObj["aggregation"];
    std::vector<std::string> groupingKeys;
    for (const auto& key : aggregation["group_by"]) {
      groupingKeys.push_back(key.asString());
    }

    std::vector<std::string> aggregates;
    for (const auto& aggregate : aggregation["aggregates"]) {
      aggregates.push_back(aggregate.asString());
    }

    const auto strategy =
        aggregation.getDefault("strategy", "single").asString();
    core::PlanNodeId nodeId;
    if (strategy == "partial_final") {
      builder.partialAggregation(groupingKeys, aggregates)
          .capturePlanNodeId(nodeId);
      tracedNodes.push_back({nodeId, "PartialAggregation"});
      builder.finalAggregation().capturePlanNodeId(nodeId);
      tracedNodes.push_back({nodeId, "Aggregation"});
    } else {
      builder.singleAggregation(groupingKeys, aggregates)
          .capturePlanNodeId(nodeId);
      tracedNodes.push_back({nodeId, "Aggregation"});
    }
  }

  if (planObj.count("order_by") > 0 && !planObj["order_by"].empty()) {
    std::vector<std::string> orderBy;
    for (const auto& key : planObj["order_by"]) {
      orderBy.push_back(key.asString());
    }

    if (planObj.count("limit") > 0 &&
        planObj["limit"].getDefault("offset", 0).asInt() == 0) {
      builder.topN(
          orderBy,
          static_cast<int32_t>(planObj["limit"]["count"].asInt()),
          false);
    } else {
      builder.orderBy(orderBy, false);
    }
  }

  if (planObj.count("limit") > 0 &&
      !(planObj.count("order_by") > 0 && !planObj["order_by"].empty() &&
        planObj["limit"].getDefault("offset", 0).asInt() == 0)) {
    builder.limit(
        planObj["limit"].getDefault("offset", 0).asInt(),
        planObj["limit"]["count"].asInt(),
        false);
  }

  const auto plan = builder.planNode();
  exec::trace::createTraceDirectory(options.traceRoot);

  auto start = std::chrono::steady_clock::now();
  std::shared_ptr<exec::Task> task;
  auto resultVector =
      exec::test::AssertQueryBuilder(plan)
          .maxDrivers(maxDrivers)
          .taskId(queryId + "_task")
          .config(core::QueryConfig::kQueryTraceDir, options.traceRoot)
          .config(
              core::QueryConfig::kQueryTraceNodeIds,
              joinComma(collectTraceNodeIds(tracedNodes)))
          .copyResults(pool.get(), task);
  auto end = std::chrono::steady_clock::now();

  folly::dynamic result = folly::dynamic::object;
  result["query_id"] = queryId;
  result["task_id"] = task->taskId();
  result["task_trace_dir"] =
      exec::trace::getTaskTraceDirectory(options.traceRoot, *task);
  result["trace_root"] = options.traceRoot;
  result["runtime_ms"] =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  result["row_count"] = resultVector->size();
  result["rows"] = rowsToJson(resultVector);
  result["traced_nodes"] = tracedNodesJson(tracedNodes);
  result["plan_string"] = plan->toString(true, true, true);
  result["request"] = request;

  std::cout << folly::toJson(result) << std::endl;
  return 0;
}

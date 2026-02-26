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

#include "bolt/common/memory/sparksql/OomPrinter.h"

#include <fmt/core.h>
#include <optional>

#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
namespace bytedance::bolt::memory::sparksql {

namespace {
std::optional<int64_t> extractTaskId(const std::string& rootPoolName) {
  // rootPoolName pattern is:
  // ShuffleReader_TID_263_HANDLER_140134287697920_root
  size_t startPos = rootPoolName.find("TID_");
  if (startPos == std::string::npos) {
    return std::nullopt;
  }
  startPos += 4;

  bool isNegative = false;
  if (startPos < rootPoolName.size() && rootPoolName[startPos] == '-') {
    isNegative = true;
    startPos++;
  }

  if (startPos >= rootPoolName.size() ||
      !std::isdigit(rootPoolName[startPos])) {
    return std::nullopt;
  }

  size_t endPos = startPos;
  while (endPos < rootPoolName.size() && std::isdigit(rootPoolName[endPos])) {
    ++endPos;
  }

  try {
    std::string numStr = isNegative ? "-" : "";
    numStr += rootPoolName.substr(startPos, endPos - startPos);
    return std::stoi(numStr);
  } catch (...) {
    return std::nullopt;
  }
}

std::string normalizeTreeString(std::string& tree) {
  std::string result;
  const std::string indent = "  ";

  // remove \n in the end of line
  while (!tree.empty() && tree.back() == '\n') {
    tree.pop_back();
  }

  result.reserve(tree.size() * 2);
  for (char c : tree) {
    // because tree in level 2, so need add 1 indent at the begin of line
    result += (c == '\n') ? "\n" + indent : std::string(1, c);
  }
  // the first line of tree also need indent
  return indent + result;
}

} // namespace

void OomPrinter::linkHolders(
    std::map<BoltMemoryManagerHolderKey, BoltMemoryManagerHolder*>* holders) {
  holders_ = holders;
}

bool OomPrinter::linked() {
  return holders_ != nullptr;
}

std::string OomPrinter::OomMessage(
    ::bytedance::bolt::memory::MemoryPool* pool) {
  std::string message;
  if (!linked()) {
    return message;
  }
  auto rootPoolName = pool->root()->name();
  auto taskId = extractTaskId(rootPoolName);
  if (!taskId.has_value()) {
    message += "Can't extract TaskId from root pool name: " + rootPoolName +
        ", so print all active tasks' memory managers. \n";
  }

  // should be 0, but we want to avoid divide zero error
  double totalUsed = 1, maxUsed = -1;
  std::map<int64_t, int64_t> taskUsed;
  for (const auto& pair : *holders_) {
    totalUsed += pair.second->usage();
    auto it = taskUsed.find(pair.first.taskId);
    auto specificTaskUsed =
        (it == taskUsed.end() ? 0 : it->second) + pair.second->usage();
    taskUsed[pair.first.taskId] = specificTaskUsed;
    maxUsed = std::max<double>(maxUsed, specificTaskUsed);
  }

  std::string otherUsed = fmt::format(
      "Total used {}, max used {}\n",
      succinctBytes(totalUsed),
      succinctBytes(maxUsed));

  for (const auto& pair : taskUsed) {
    otherUsed += fmt::format(
        "Task {} used {}, {:.2f}% \n",
        pair.first,
        succinctBytes(pair.second),
        pair.second / totalUsed * 100);
  }

  std::vector<BoltMemoryManagerHolderKey> keys;
  for (const auto& pair : *holders_) {
    const auto& key = pair.first;

    if (taskId.has_value() && key.taskId != taskId) {
      continue;
    }

    keys.push_back(key);
  }

  // sort by mem usage
  std::sort(
      keys.begin(),
      keys.end(),
      [&](const BoltMemoryManagerHolderKey& left,
          const BoltMemoryManagerHolderKey& right) {
        const auto& leftValue = holders_->at(left);
        const auto& rightValue = holders_->at(right);
        return leftValue->usage() > rightValue->usage();
      });

  for (int i = 0; i < keys.size(); i++) {
    const auto& key = keys[i];
    const auto& value = holders_->at(key);

    auto totalUsed = succinctBytes(value->usage());
    message += fmt::format(
        "{} reserve {} usage {} peak {}\n",
        key.name,
        totalUsed,
        totalUsed,
        totalUsed);

    std::string tree = value->getManager()->treeMemoryUsage();
    message += normalizeTreeString(tree);

    // prepare for next print
    message += "\n";
  }

  const std::string messageDelimeter = "\n\n";

  auto instance = ExecutionMemoryPool::instance();

  return messageDelimeter + message + messageDelimeter + otherUsed +
      messageDelimeter +
      (instance == nullptr ? "ExecutionMemoryPool is nullptr"
                           : instance->toString()) +
      messageDelimeter;
}
} // namespace bytedance::bolt::memory::sparksql

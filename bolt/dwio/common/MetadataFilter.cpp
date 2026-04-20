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

#include "bolt/dwio/common/MetadataFilter.h"

#include <folly/container/F14Map.h>
#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
namespace bytedance::bolt::common {

namespace {
using LeafResults =
    folly::F14FastMap<const MetadataFilter::LeafNode*, std::vector<uint64_t>*>;
}

struct MetadataFilter::Node {
  static std::unique_ptr<Node> fromExpression(
      const core::ITypedExpr&,
      core::ExpressionEvaluator*,
      bool negated,
      bool enableMapSubscriptFilter);
  virtual ~Node() = default;
  virtual void addToScanSpec(ScanSpec&) const = 0;
  virtual uint64_t* eval(LeafResults&, int size) const = 0;
  virtual std::string toString() const = 0;
};

class MetadataFilter::LeafNode : public Node {
 public:
  LeafNode(Subfield&& field, std::unique_ptr<Filter> filter)
      : field_(std::move(field)), filter_(std::move(filter)) {}

  void addToScanSpec(ScanSpec& scanSpec) const override {
    scanSpec.getOrCreateChild(field_)->addMetadataFilter(this, filter_.get());
  }

  uint64_t* eval(LeafResults& leafResults, int) const override {
    if (auto it = leafResults.find(this); it != leafResults.end()) {
      return it->second->data();
    }
    return nullptr;
  }

  const Subfield& field() const {
    return field_;
  }

  std::string toString() const override {
    return field_.toString() + ":" + filter_->toString();
  }

 private:
  Subfield field_;
  std::unique_ptr<Filter> filter_;
};

struct MetadataFilter::AndNode : Node {
  static std::unique_ptr<Node> create(
      std::unique_ptr<Node> lhs,
      std::unique_ptr<Node> rhs) {
    if (!lhs) {
      return rhs;
    }
    if (!rhs) {
      return lhs;
    }
    return std::make_unique<AndNode>(std::move(lhs), std::move(rhs));
  }

  AndNode(std::unique_ptr<Node> lhs, std::unique_ptr<Node> rhs)
      : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

  void addToScanSpec(ScanSpec& scanSpec) const override {
    lhs_->addToScanSpec(scanSpec);
    rhs_->addToScanSpec(scanSpec);
  }

  uint64_t* eval(LeafResults& leafResults, int size) const override {
    auto* l = lhs_->eval(leafResults, size);
    auto* r = rhs_->eval(leafResults, size);
    if (!l) {
      return r;
    }
    if (!r) {
      return l;
    }
    bits::orBits(l, r, 0, size);
    return l;
  }

  std::string toString() const override {
    return "and(" + lhs_->toString() + "," + rhs_->toString() + ")";
  }

 private:
  std::unique_ptr<Node> lhs_;
  std::unique_ptr<Node> rhs_;
};

struct MetadataFilter::OrNode : Node {
  static std::unique_ptr<Node> create(
      std::unique_ptr<Node> lhs,
      std::unique_ptr<Node> rhs) {
    if (!lhs || !rhs) {
      return nullptr;
    }
    return std::make_unique<OrNode>(std::move(lhs), std::move(rhs));
  }

  OrNode(std::unique_ptr<Node> lhs, std::unique_ptr<Node> rhs)
      : lhs_(std::move(lhs)), rhs_(std::move(rhs)) {}

  void addToScanSpec(ScanSpec& scanSpec) const override {
    lhs_->addToScanSpec(scanSpec);
    rhs_->addToScanSpec(scanSpec);
  }

  uint64_t* eval(LeafResults& leafResults, int size) const override {
    auto* l = lhs_->eval(leafResults, size);
    auto* r = rhs_->eval(leafResults, size);
    if (!l || !r) {
      return nullptr;
    }
    bits::andBits(l, r, 0, size);
    return l;
  }

  std::string toString() const override {
    return "or(" + lhs_->toString() + "," + rhs_->toString() + ")";
  }

 private:
  std::unique_ptr<Node> lhs_;
  std::unique_ptr<Node> rhs_;
};

namespace {

const core::CallTypedExpr* asCall(const core::ITypedExpr* expr) {
  return dynamic_cast<const core::CallTypedExpr*>(expr);
}

} // namespace

std::unique_ptr<MetadataFilter::Node> MetadataFilter::Node::fromExpression(
    const core::ITypedExpr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated,
    bool enableMapSubscriptFilter) {
  auto* call = asCall(&expr);
  if (!call) {
    return nullptr;
  }
  if (call->name() == "and") {
    auto lhs = fromExpression(
        *call->inputs()[0], evaluator, negated, enableMapSubscriptFilter);
    auto rhs = fromExpression(
        *call->inputs()[1], evaluator, negated, enableMapSubscriptFilter);
    return negated ? OrNode::create(std::move(lhs), std::move(rhs))
                   : AndNode::create(std::move(lhs), std::move(rhs));
  }
  if (call->name() == "or") {
    auto lhs = fromExpression(
        *call->inputs()[0], evaluator, negated, enableMapSubscriptFilter);
    auto rhs = fromExpression(
        *call->inputs()[1], evaluator, negated, enableMapSubscriptFilter);
    return negated ? AndNode::create(std::move(lhs), std::move(rhs))
                   : OrNode::create(std::move(lhs), std::move(rhs));
  }
  if (call->name() == "not") {
    return fromExpression(
        *call->inputs()[0], evaluator, !negated, enableMapSubscriptFilter);
  }
  try {
    auto subfieldAndFilter =
        exec::ExprToSubfieldFilterParser::getInstance()
            ->leafCallToSubfieldFilter(*call, evaluator, negated);
    if (!subfieldAndFilter.has_value()) {
      return nullptr;
    }

    auto& [subfield, filter] = subfieldAndFilter.value();
    if (filter->kind() == FilterKind::kMapSubscript &&
        !enableMapSubscriptFilter) {
      return nullptr;
    }
    BOLT_CHECK(
        subfield.valid(),
        "Invalid subfield from expression: {}",
        expr.toString());
    return std::make_unique<LeafNode>(std::move(subfield), std::move(filter));
  } catch (const BoltException&) {
    LOG(WARNING) << "Fail to convert expression to metadata filter: "
                 << expr.toString();
    return nullptr;
  }
}

MetadataFilter::MetadataFilter(
    ScanSpec& scanSpec,
    const core::ITypedExpr& expr,
    core::ExpressionEvaluator* evaluator,
    bool enableMapSubscriptFilter)
    : root_(Node::fromExpression(
          expr,
          evaluator,
          false,
          enableMapSubscriptFilter)) {
  if (root_) {
    root_->addToScanSpec(scanSpec);
  }
}

void MetadataFilter::eval(
    std::vector<std::pair<const LeafNode*, std::vector<uint64_t>>>&
        leafNodeResults,
    std::vector<uint64_t>& finalResult) {
  if (!root_) {
    return;
  }
  LeafResults leafResults;
  for (auto& [leaf, result] : leafNodeResults) {
    BOLT_CHECK_EQ(
        result.size(),
        finalResult.size(),
        "Result size mismatch: {}",
        leaf->field().toString());
    BOLT_CHECK(
        leafResults.emplace(leaf, &result).second,
        "Duplicate results: {}",
        leaf->field().toString());
  }
  const auto bitCount = finalResult.size() * 64;
  if (auto* combined = root_->eval(leafResults, bitCount)) {
    bits::orBits(finalResult.data(), combined, 0, bitCount);
  }
}

std::string MetadataFilter::toString() const {
  return !root_ ? "" : root_->toString();
}

} // namespace bytedance::bolt::common

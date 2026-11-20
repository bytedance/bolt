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

#include "bolt/substrait/SubstraitExtensionCollector.h"
namespace bytedance::bolt::substrait {

int SubstraitExtensionCollector::getReferenceNumber(
    const std::string& functionName,
    const std::vector<TypePtr>& arguments) {
  const auto& substraitFunctionSignature =
      BoltSubstraitSignature::toSubstraitSignature(functionName, arguments);
  // TODO: Currently we treat all bolt registry based function signatures as
  // custom substrait extension, so no uri link and leave it as empty.
  return getReferenceNumber({"", substraitFunctionSignature});
}

int SubstraitExtensionCollector::getReferenceNumber(
    const std::string& functionName,
    const std::vector<TypePtr>& arguments,
    const core::AggregationNode::Step /* aggregationStep */) {
  // TODO: Ignore aggregationStep for now, will refactor when introduce bolt
  // registry for function signature binding
  return getReferenceNumber(functionName, arguments);
}

template <typename T>
bool SubstraitExtensionCollector::BiDirectionHashMap<T>::putIfAbsent(
    const int& key,
    const T& value) {
  if (forwardMap_.find(key) == forwardMap_.end() &&
      reverseMap_.find(value) == reverseMap_.end()) {
    forwardMap_[key] = value;
    reverseMap_[value] = key;
    return true;
  }
  return false;
}

void SubstraitExtensionCollector::addExtensionsToPlan(
    ::substrait::Plan* plan) const {
  using SimpleExtensionURI = ::substrait::extensions::SimpleExtensionURI;
  // Currently we don't introduce any substrait extension YAML files, so always
  // only have one URI.
  SimpleExtensionURI* extensionUri = plan->add_extension_uris();
  extensionUri->set_extension_uri_anchor(1);

  for (const auto& [referenceNum, functionId] :
       extensionFunctions_->forwardMap()) {
    auto extensionFunction =
        plan->add_extensions()->mutable_extension_function();
    extensionFunction->set_extension_uri_reference(
        extensionUri->extension_uri_anchor());
    extensionFunction->set_function_anchor(referenceNum);
    extensionFunction->set_name(functionId.signature);
  }

  // Add type extensions collected during conversion.
  for (const auto& [anchor, typeName] : typeExtensions_->forwardMap()) {
    auto extensionType = plan->add_extensions()->mutable_extension_type();
    extensionType->set_extension_uri_reference(
        extensionUri->extension_uri_anchor());
    extensionType->set_type_anchor(anchor);
    extensionType->set_name(typeName);
  }
}

SubstraitExtensionCollector::SubstraitExtensionCollector() {
  extensionFunctions_ =
      std::make_shared<BiDirectionHashMap<ExtensionFunctionId>>();
  typeExtensions_ = std::make_shared<BiDirectionHashMap<std::string>>();
}

SubstraitExtensionCollector::SubstraitExtensionCollector(
    const std::vector<::substrait::extensions::SimpleExtensionDeclaration>&
        extensions) {
  // Initialize maps.
  extensionFunctions_ =
      std::make_shared<BiDirectionHashMap<ExtensionFunctionId>>();
  typeExtensions_ = std::make_shared<BiDirectionHashMap<std::string>>();

  // Seed maps using provided declarations and advance anchor counters.
  for (const auto& decl : extensions) {
    if (decl.has_extension_function()) {
      const auto& fn = decl.extension_function();
      const int funcAnchor = fn.function_anchor();
      const std::string signature = fn.name();
      // URI is not tracked currently; leave empty to match existing behavior.
      ExtensionFunctionId id{"", signature};
      extensionFunctions_->putIfAbsent(funcAnchor, id);
      if (funcAnchor > functionReferenceNumber_) {
        functionReferenceNumber_ = funcAnchor;
      }
    } else if (decl.has_extension_type()) {
      const auto& typeDecl = decl.extension_type();
      const int typeAnchor = static_cast<int>(typeDecl.type_anchor());
      const std::string typeName = typeDecl.name();
      if (!typeName.empty()) {
        typeExtensions_->putIfAbsent(typeAnchor, typeName);
        if (typeAnchor > typeReferenceNumber_) {
          typeReferenceNumber_ = typeAnchor;
        }
      }
    }
  }
}

int SubstraitExtensionCollector::getReferenceNumber(
    const ExtensionFunctionId& extensionFunctionId) {
  const auto& extensionFunctionAnchorIt =
      extensionFunctions_->reverseMap().find(extensionFunctionId);
  if (extensionFunctionAnchorIt != extensionFunctions_->reverseMap().end()) {
    return extensionFunctionAnchorIt->second;
  }
  ++functionReferenceNumber_;
  extensionFunctions_->putIfAbsent(
      functionReferenceNumber_, extensionFunctionId);
  return functionReferenceNumber_;
}

int SubstraitExtensionCollector::getTypeAnchor(const std::string& typeName) {
  // Return existing anchor if present.
  const auto& it = typeExtensions_->reverseMap().find(typeName);
  if (it != typeExtensions_->reverseMap().end()) {
    return it->second;
  }
  // Legacy null literals use anchor 0 for UNKNOWN. Respect an existing
  // anchor when seeded from a plan; otherwise reserve 0 when available.
  if (typeName == "UNKNOWN" && typeExtensions_->forwardMap().count(0) == 0) {
    typeExtensions_->putIfAbsent(0, typeName);
    return 0;
  }
  ++typeReferenceNumber_;
  typeExtensions_->putIfAbsent(typeReferenceNumber_, typeName);
  return typeReferenceNumber_;
}

} // namespace bytedance::bolt::substrait

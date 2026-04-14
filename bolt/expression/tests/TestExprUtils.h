#pragma once

#include "bolt/core/Expressions.h"

namespace bytedance::bolt::exec::test {

inline core::CallTypedExprPtr renameCall(
    const core::TypedExprPtr& expr,
    const std::string& name) {
  auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr);
  BOLT_CHECK_NOT_NULL(call);
  return std::make_shared<core::CallTypedExpr>(
      call->type(),
      std::vector<core::TypedExprPtr>(
          call->inputs().begin(), call->inputs().end()),
      name);
}

} // namespace bytedance::bolt::exec::test

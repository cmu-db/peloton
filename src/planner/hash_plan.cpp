//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_plan.cpp
//
// Identification: src/planner/hash_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/hash_plan.h"

namespace peloton {
namespace planner {

void HashPlan::PerformBinding(BindingContext &binding_context) {
  // Let the children bind
  AbstractPlan::PerformBinding(binding_context);

  // Now us
  for (auto &hash_key : hash_keys_) {
    auto *key_expr =
        const_cast<expression::AbstractExpression *>(hash_key.get());
    key_expr->PerformBinding({&binding_context});
  }
}

}  // namespace planner
}  // namespace peloton
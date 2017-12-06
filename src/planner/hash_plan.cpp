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

void HashPlan::GetOutputColumns (std::vector<oid_t> &columns) const {
  GetChild(0)->GetOutputColumns(columns);
}

hash_t HashPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  for (auto &hash_key : hash_keys_)
    hash = HashUtil::CombineHashes(hash, hash_key->Hash());

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool HashPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;
 
  auto &other = static_cast<const planner::HashPlan &>(rhs);
  auto hash_key_size = GetHashKeys().size();
  if (hash_key_size != other.GetHashKeys().size())
    return false;

  for (size_t i = 0; i < hash_key_size; i++) {
    if (*GetHashKeys()[i].get() != *other.GetHashKeys()[i].get())
      return false;
  }
 
  return AbstractPlan::operator==(rhs);
}

void HashPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  for (auto &hash_key : hash_keys_) {
    auto *expr = const_cast<expression::AbstractExpression *>(hash_key.get());
    expr->VisitParameters(map, values, values_from_user);
  }
}

}  // namespace planner
}  // namespace peloton

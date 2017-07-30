//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_plan.cpp
//
// Identification: src/planner/hash_join_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "type/types.h"
#include "expression/abstract_expression.h"
#include "planner/project_info.h"

#include "planner/hash_join_plan.h"

namespace peloton {
namespace planner {

HashJoinPlan::HashJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {}

HashJoinPlan::HashJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema,
    const std::vector<oid_t>
        &outer_hashkeys)  // outer_hashkeys is added for IN-subquery
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema) {
  outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
}

void HashJoinPlan::HandleSubplanBinding(
    bool is_left, const BindingContext &input) {
  auto &keys = is_left ? left_hash_keys_ : right_hash_keys_;
  for (auto &key : keys) {
    auto *key_exp = const_cast<expression::AbstractExpression *>(key.get());
    key_exp->PerformBinding({&input});
  }
}

bool HashJoinPlan::operator==(AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = reinterpret_cast<planner::HashJoinPlan &>(rhs);
  if (GetJoinType() != other.GetJoinType())
    return false;

  // Prodicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr))
    return false;
  if (pred && *pred != *other_pred)
    return false;

  // Project Info
  auto *proj_info = GetProjInfo();
  auto *other_proj_info = other.GetProjInfo();
  if ((proj_info == nullptr && other_proj_info != nullptr) ||
      (proj_info != nullptr && other_proj_info == nullptr))
    return false;
  if (proj_info && *proj_info != *other_proj_info)
    return false;

  // Left hash keys
  std::vector<const expression::AbstractExpression *> keys, other_keys;
  GetLeftHashKeys(keys);
  other.GetLeftHashKeys(other_keys);
  size_t keys_count = keys.size();
  if (keys_count != other_keys.size())
    return false;
  for (size_t i = 0; i < keys_count; i++) {
    if (*keys.at(i) != *other_keys.at(i))
      return false;
  }

  // Right hash keys
  keys.clear();
  other_keys.clear();
  GetRightHashKeys(keys);
  other.GetRightHashKeys(other_keys);
  keys_count = keys.size();
  if (keys_count != other_keys.size())
    return false;
  for (size_t i = 0; i < keys_count; i++) {
    if (*keys.at(i) != *other_keys.at(i))
      return false;
  }

  return AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton

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

#include "expression/abstract_expression.h"
#include "planner/project_info.h"
#include "type/types.h"

#include "planner/hash_join_plan.h"

namespace peloton {
namespace planner {

HashJoinPlan::HashJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema, bool build_bloomfilter)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema),
      build_bloomfilter_(build_bloomfilter) {}

// outer_hashkeys is added for IN-subquery
HashJoinPlan::HashJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema,
    const std::vector<oid_t> &outer_hashkeys, bool build_bloomfilter)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema),
      build_bloomfilter_(build_bloomfilter) {
  outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
}

HashJoinPlan::HashJoinPlan(
    JoinType join_type,
    std::unique_ptr<const expression::AbstractExpression> &&predicate,
    std::unique_ptr<const ProjectInfo> &&proj_info,
    std::shared_ptr<const catalog::Schema> &proj_schema,
    std::vector<std::unique_ptr<const expression::AbstractExpression>>
        &left_hash_keys,
    std::vector<std::unique_ptr<const expression::AbstractExpression>>
        &right_hash_keys,
    bool build_bloomfilter)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema),
      left_hash_keys_(std::move(left_hash_keys)),
      right_hash_keys_(std::move(right_hash_keys)),
      build_bloomfilter_(build_bloomfilter) {}

void HashJoinPlan::HandleSubplanBinding(bool is_left,
                                        const BindingContext &input) {
  auto &keys = is_left ? left_hash_keys_ : right_hash_keys_;
  for (auto &key : keys) {
    auto *key_exp = const_cast<expression::AbstractExpression *>(key.get());
    key_exp->PerformBinding({&input});
  }
}

bool HashJoinPlan::Equals(planner::AbstractPlan &plan) const {
  if (GetPlanNodeType() != plan.GetPlanNodeType())
    return false;

  auto &other = reinterpret_cast<planner::HashJoinPlan &>(plan);
  // compare join_type
  if (GetJoinType() != other.GetJoinType())
    return false;

  // compare predicate
  auto *exp = GetPredicate();
  if (exp && !exp->Equals(other.GetPredicate()))
    return false;

  // compare proj_info
  auto *proj_info = GetProjInfo();
  if (proj_info && !proj_info->Equals(*other.GetProjInfo()))
    return false;

  // compare proj_schema
  if (*GetSchema() != *other.GetSchema())
    return false;

  std::vector<const expression::AbstractExpression *> keys, target_keys;
  GetLeftHashKeys(keys);
  other.GetLeftHashKeys(target_keys);
  size_t keys_count = keys.size();
  if (keys_count != target_keys.size())
    return false;
  for (size_t i = 0; i < keys_count; i++) {
    if (!keys.at(i)->Equals(target_keys.at(i)))
      return false;
  }

  keys.clear();
  target_keys.clear();
  GetRightHashKeys(keys);
  other.GetRightHashKeys(target_keys);
  keys_count = keys.size();
  if (keys_count != target_keys.size())
    return false;
  for (size_t i = 0; i < keys_count; i++) {
    if (!keys.at(i)->Equals(target_keys.at(i)))
      return false;
  }

  return AbstractPlan::Equals(plan);
}

}  // namespace planner
}  // namespace peloton

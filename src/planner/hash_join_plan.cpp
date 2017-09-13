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

hash_t HashJoinPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  auto join_type = GetJoinType();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&join_type));
  if (GetPredicate() != nullptr)
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  if (GetProjInfo() != nullptr)
    hash = HashUtil::CombineHashes(hash, GetProjInfo()->Hash());
  std::vector<const expression::AbstractExpression *> keys;
  GetLeftHashKeys(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    hash = HashUtil::CombineHashes(hash, keys.at(i)->Hash());
  }
  keys.clear();
  GetRightHashKeys(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    hash = HashUtil::CombineHashes(hash, keys.at(i)->Hash());
  }
  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool HashJoinPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::HashJoinPlan &>(rhs);
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

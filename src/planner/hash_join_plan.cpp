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
#include "common/internal_types.h"

#include "planner/hash_join_plan.h"

namespace peloton {
namespace planner {

HashJoinPlan::HashJoinPlan(JoinType join_type, ExpressionPtr &&predicate,
                           std::unique_ptr<const ProjectInfo> &&proj_info,
                           std::shared_ptr<const catalog::Schema> &proj_schema,
                           bool build_bloomfilter)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema),
      build_bloomfilter_(build_bloomfilter) {}

// outer_hashkeys is added for IN-subquery
HashJoinPlan::HashJoinPlan(JoinType join_type, ExpressionPtr &&predicate,
                           std::unique_ptr<const ProjectInfo> &&proj_info,
                           std::shared_ptr<const catalog::Schema> &proj_schema,
                           const std::vector<oid_t> &outer_hashkeys,
                           bool build_bloomfilter)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema),
      build_bloomfilter_(build_bloomfilter) {
  outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
}

HashJoinPlan::HashJoinPlan(JoinType join_type, ExpressionPtr &&predicate,
                           std::unique_ptr<const ProjectInfo> &&proj_info,
                           std::shared_ptr<const catalog::Schema> &proj_schema,
                           std::vector<ExpressionPtr> &left_hash_keys,
                           std::vector<ExpressionPtr> &right_hash_keys,
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
  hash_t hash = AbstractJoinPlan::Hash();

  std::vector<const expression::AbstractExpression *> keys;
  GetLeftHashKeys(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    hash = HashUtil::CombineHashes(hash, keys[i]->Hash());
  }

  keys.clear();
  GetRightHashKeys(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    hash = HashUtil::CombineHashes(hash, keys[i]->Hash());
  }

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool HashJoinPlan::operator==(const AbstractPlan &rhs) const {
  if (!AbstractJoinPlan::operator==(rhs)) {
    return false;
  }

  const auto &other = static_cast<const HashJoinPlan &>(rhs);

  std::vector<const expression::AbstractExpression *> keys, other_keys;

  // Left hash keys
  GetLeftHashKeys(keys);
  other.GetLeftHashKeys(other_keys);
  size_t keys_count = keys.size();
  if (keys_count != other_keys.size()) {
    return false;
  }

  for (size_t i = 0; i < keys_count; i++) {
    if (*keys[i] != *other_keys[i]) {
      return false;
    }
  }

  keys.clear();
  other_keys.clear();

  // Right hash keys
  GetRightHashKeys(keys);
  other.GetRightHashKeys(other_keys);
  keys_count = keys.size();
  if (keys_count != other_keys.size()) {
    return false;
  }

  for (size_t i = 0; i < keys_count; i++) {
    if (*keys[i] != *other_keys[i]) {
      return false;
    }
  }

  return AbstractPlan::operator==(rhs);
}

void HashJoinPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  AbstractPlan::VisitParameters(map, values, values_from_user);

  std::vector<const expression::AbstractExpression *> left_hash_keys;
  GetLeftHashKeys(left_hash_keys);
  for (auto left_hash_key : left_hash_keys) {
    auto key = const_cast<expression::AbstractExpression *>(left_hash_key);
    key->VisitParameters(map, values, values_from_user);
  }

  std::vector<const expression::AbstractExpression *> right_hash_keys;
  GetLeftHashKeys(right_hash_keys);
  for (auto right_hash_key : right_hash_keys) {
    auto key = const_cast<expression::AbstractExpression *>(right_hash_key);
    key->VisitParameters(map, values, values_from_user);
  }

  auto predicate = const_cast<expression::AbstractExpression *>(GetPredicate());
  if (predicate != nullptr) {
    predicate->VisitParameters(map, values, values_from_user);
  }
}

}  // namespace planner
}  // namespace peloton

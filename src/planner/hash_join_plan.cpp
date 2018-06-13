//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_plan.cpp
//
// Identification: src/planner/hash_join_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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
                           std::vector<ExpressionPtr> &left_hash_keys,
                           std::vector<ExpressionPtr> &right_hash_keys,
                           bool build_bloomfilter)
    : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                       proj_schema),
      left_hash_keys_(std::move(left_hash_keys)),
      right_hash_keys_(std::move(right_hash_keys)),
      build_bloomfilter_(build_bloomfilter) {}

void HashJoinPlan::GetLeftHashKeys(
    std::vector<const expression::AbstractExpression *> &keys) const {
  for (const auto &left_key : left_hash_keys_) {
    keys.push_back(left_key.get());
  }
}

void HashJoinPlan::GetRightHashKeys(
    std::vector<const expression::AbstractExpression *> &keys) const {
  for (const auto &right_key : right_hash_keys_) {
    keys.push_back(right_key.get());
  }
}

void HashJoinPlan::HandleSubplanBinding(bool is_left,
                                        const BindingContext &input) {
  auto &keys = is_left ? left_hash_keys_ : right_hash_keys_;
  for (auto &key : keys) {
    auto *key_exp = const_cast<expression::AbstractExpression *>(key.get());
    key_exp->PerformBinding({&input});
  }
}

std::unique_ptr<AbstractPlan> HashJoinPlan::Copy() const {
  // Predicate
  ExpressionPtr predicate_copy(GetPredicate() ? GetPredicate()->Copy()
                                              : nullptr);

  // Schema
  std::shared_ptr<const catalog::Schema> schema_copy(
      catalog::Schema::CopySchema(GetSchema()));

  // Left and right hash keys
  std::vector<ExpressionPtr> left_hash_keys_copy, right_hash_keys_copy;
  for (const auto &left_hash_key : left_hash_keys_) {
    left_hash_keys_copy.emplace_back(left_hash_key->Copy());
  }
  for (const auto &right_hash_key : right_hash_keys_) {
    right_hash_keys_copy.emplace_back(right_hash_key->Copy());
  }

  // Create plan copy
  auto *new_plan =
      new HashJoinPlan(GetJoinType(), std::move(predicate_copy),
                       GetProjInfo()->Copy(), schema_copy, left_hash_keys_copy,
                       right_hash_keys_copy, build_bloomfilter_);
  return std::unique_ptr<AbstractPlan>(new_plan);
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

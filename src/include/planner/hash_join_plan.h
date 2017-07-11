//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_plan.h
//
// Identification: src/include/planner/hash_join_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numeric>

#include "planner/abstract_join_plan.h"

namespace peloton {
namespace expression {
class AbstractExpression;
}
namespace planner {

class HashJoinPlan : public AbstractJoinPlan {
 public:
  HashJoinPlan(
      JoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema);

  HashJoinPlan(
      JoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      const std::vector<oid_t> &outer_hashkeys);

  HashJoinPlan(
      JoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      std::vector<std::unique_ptr<const expression::AbstractExpression>> &
          left_hash_keys,
      std::vector<std::unique_ptr<const expression::AbstractExpression>> &
          right_hash_keys)
      : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                         proj_schema),
        left_hash_keys_(std::move(left_hash_keys)),
        right_hash_keys_(std::move(right_hash_keys)) {}

  void HandleSubplanBinding(bool is_left, const BindingContext &input) override;

  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::HASHJOIN; }

  void GetOutputColumns(std::vector<oid_t> &columns) const override {
    columns.resize(GetSchema()->GetColumnCount());
    std::iota(columns.begin(), columns.end(), 0);
  }

  const std::string GetInfo() const override { return "HashJoin"; }

  const std::vector<oid_t> &GetOuterHashIds() const {
    return outer_column_ids_;
  }

  void GetLeftHashKeys(
      std::vector<const expression::AbstractExpression *> &keys) const {
    for (const auto &left_key : left_hash_keys_) {
      keys.push_back(left_key.get());
    }
  }

  void GetRightHashKeys(
      std::vector<const expression::AbstractExpression *> &keys) const {
    for (const auto &right_key : right_hash_keys_) {
      keys.push_back(right_key.get());
    }
  }

  std::unique_ptr<AbstractPlan> Copy() const override {
    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        GetPredicate()->Copy());
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));
    HashJoinPlan *new_plan = new HashJoinPlan(
        GetJoinType(), std::move(predicate_copy),
        GetProjInfo()->Copy(), schema_copy, outer_column_ids_);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  std::vector<oid_t> outer_column_ids_;

  std::vector<std::unique_ptr<const expression::AbstractExpression>>
      left_hash_keys_;
  std::vector<std::unique_ptr<const expression::AbstractExpression>>
      right_hash_keys_;

 private:
  DISALLOW_COPY_AND_MOVE(HashJoinPlan);
};

}  // namespace planner
}  // namespace peloton

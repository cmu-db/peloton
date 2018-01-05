//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_plan.h
//
// Identification: src/include/planner/hash_join_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numeric>

#include "planner/abstract_join_plan.h"

namespace peloton {

namespace expression {
class AbstractExpression;
class Parameter;
}  // namespace expression

namespace planner {

class HashJoinPlan : public AbstractJoinPlan {
  using ExpressionPtr = std::unique_ptr<const expression::AbstractExpression>;

 public:
  HashJoinPlan(JoinType join_type, ExpressionPtr &&predicate,
               std::unique_ptr<const ProjectInfo> &&proj_info,
               std::shared_ptr<const catalog::Schema> &proj_schema,
               bool build_bloomfilter = false);

  HashJoinPlan(JoinType join_type, ExpressionPtr &&predicate,
               std::unique_ptr<const ProjectInfo> &&proj_info,
               std::shared_ptr<const catalog::Schema> &proj_schema,
               const std::vector<oid_t> &outer_hashkeys,
               bool build_bloomfilter = false);

  HashJoinPlan(JoinType join_type, ExpressionPtr &&predicate,
               std::unique_ptr<const ProjectInfo> &&proj_info,
               std::shared_ptr<const catalog::Schema> &proj_schema,
               std::vector<ExpressionPtr> &left_hash_keys,
               std::vector<ExpressionPtr> &right_hash_keys,
               bool build_bloomfilter = false);

  void HandleSubplanBinding(bool is_left, const BindingContext &input) override;

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::HASHJOIN;
  }

  bool IsBloomFilterEnabled() const { return build_bloomfilter_; }

  void SetBloomFilterFlag(bool flag) { build_bloomfilter_ = flag; }

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
    ExpressionPtr predicate_copy(GetPredicate() ? GetPredicate()->Copy()
                                                : nullptr);
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));
    HashJoinPlan *new_plan = new HashJoinPlan(
        GetJoinType(), std::move(predicate_copy), GetProjInfo()->Copy(),
        schema_copy, outer_column_ids_, build_bloomfilter_);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  std::vector<oid_t> outer_column_ids_;

  std::vector<ExpressionPtr> left_hash_keys_;

  std::vector<ExpressionPtr> right_hash_keys_;

  bool build_bloomfilter_;

 private:
  DISALLOW_COPY_AND_MOVE(HashJoinPlan);
};

}  // namespace planner
}  // namespace peloton

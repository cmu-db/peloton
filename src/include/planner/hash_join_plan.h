//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_plan.h
//
// Identification: src/include/planner/hash_join_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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
               std::vector<ExpressionPtr> &left_hash_keys,
               std::vector<ExpressionPtr> &right_hash_keys,
               bool build_bloomfilter = false);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(HashJoinPlan);

  void HandleSubplanBinding(bool is_left, const BindingContext &input) override;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::HASHJOIN;
  }

  bool IsBloomFilterEnabled() const { return build_bloomfilter_; }

  void SetBloomFilterFlag(bool flag) { build_bloomfilter_ = flag; }

  const std::string GetInfo() const override { return "HashJoinPlan"; }

  void GetLeftHashKeys(
      std::vector<const expression::AbstractExpression *> &keys) const;

  void GetRightHashKeys(
      std::vector<const expression::AbstractExpression *> &keys) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utils
  ///
  //////////////////////////////////////////////////////////////////////////////

  std::unique_ptr<AbstractPlan> Copy() const override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  // The left and right expressions that constitute the join keys
  std::vector<ExpressionPtr> left_hash_keys_;
  std::vector<ExpressionPtr> right_hash_keys_;

  // Flag indicating whether we build a bloom filter
  bool build_bloomfilter_;
};

}  // namespace planner
}  // namespace peloton

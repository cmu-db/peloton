//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_plan.h
//
// Identification: src/include/planner/nested_loop_join_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_join_plan.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}  // namespace expression

namespace planner {

class ProjectInfo;

class NestedLoopJoinPlan : public AbstractJoinPlan {
 public:
  NestedLoopJoinPlan(
      JoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      const std::vector<oid_t> &join_column_ids_left,
      const std::vector<oid_t> &join_column_ids_right);

  void HandleSubplanBinding(bool from_left, const BindingContext &ctx) override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::NESTLOOP;
  }

  const std::string GetInfo() const override { return "NestedLoopJoin"; }

  std::unique_ptr<AbstractPlan> Copy() const override;

  const std::vector<oid_t> &GetJoinColumnsLeft() const {
    return join_column_ids_left_;
  }

  const std::vector<const planner::AttributeInfo *> GetJoinAIsLeft() const {
    return join_ais_left_;
  }

  const std::vector<oid_t> &GetJoinColumnsRight() const {
    return join_column_ids_right_;
  }

  const std::vector<const planner::AttributeInfo *> GetJoinAIsRight() const {
    return join_ais_right_;
  }

 private:
  // columns in left table for join predicate. Note: this is columns in the
  // result. For example, you want to find column id 5 in the table, but the
  // corresponding column id in the result is column 3, then the value should
  // be 3 in  join_column_ids_left. The is specified when creating the plan.
  std::vector<oid_t> join_column_ids_left_;
  std::vector<const planner::AttributeInfo *> join_ais_left_;

  // columns in right table for join predicate. This is also the columns in the
  // result. For example, you wan to do i_id = s_id, s_id must be in the output
  // result of right child. Let's say s_id is the first column in the result.
  // Then here join_column_ids_right_ is 0. After then, we need to get the
  // physical column id and pass this physical column id to SetTupleColumnValue
  // to update the corresponding column in the index predicate
  std::vector<oid_t> join_column_ids_right_;
  std::vector<const planner::AttributeInfo *> join_ais_right_;

 private:
  DISALLOW_COPY_AND_MOVE(NestedLoopJoinPlan);
};

}  // namespace planner
}  // namespace peloton

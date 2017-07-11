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
}
namespace planner {

class ProjectInfo;

class NestedLoopJoinPlan : public AbstractJoinPlan {
 public:
  NestedLoopJoinPlan(
      JoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema);

  // This is added for passing the left and right join column ids
  NestedLoopJoinPlan(
      JoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      std::vector<oid_t> &join_column_ids_left,
      std::vector<oid_t> &join_column_ids_right);

  // Nested loops don't need to perform any attribute binding
  void HandleSubplanBinding(UNUSED_ATTRIBUTE bool,
                            UNUSED_ATTRIBUTE const BindingContext &) override {}

  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::NESTLOOP; }

  const std::string GetInfo() const override { return "NestedLoopJoin"; }

  std::unique_ptr<AbstractPlan> Copy() const override {
    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        GetPredicate()->Copy());

    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));

    NestedLoopJoinPlan *new_plan =
        new NestedLoopJoinPlan(GetJoinType(), std::move(predicate_copy),
                               GetProjInfo()->Copy(), schema_copy);

    return std::unique_ptr<AbstractPlan>(new_plan);
  }

  const std::vector<oid_t> &GetJoinColumnsLeft() const {
    return join_column_ids_left_;
  }
  const std::vector<oid_t> &GetJoinColumnsRight() const {
    return join_column_ids_right_;
  }

 private:
  // columns in left table for join predicate. Note: this is columns in the
  // result. For example, you want to find column id 5 in the table, but the
  // corresponding column id in the result is column 3, then the value should
  // be 3 in  join_column_ids_left. The is specified when creating the plan.
  std::vector<oid_t> join_column_ids_left_;

  // columns in right table for join predicate. This is also the columns in the
  // result. For example, you wan to do i_id = s_id, s_id must be in the output
  // result of right child. Let's say s_id is the first column in the result.
  // Then here join_column_ids_right_ is 0. After then, we need to get the
  // physical column id and pass this physical column id to SetTupleColumnValue
  // to update the corresponding column in the index predicate
  std::vector<oid_t> join_column_ids_right_;

 private:
  DISALLOW_COPY_AND_MOVE(NestedLoopJoinPlan);
};

}  // namespace planner
}  // namespace peloton

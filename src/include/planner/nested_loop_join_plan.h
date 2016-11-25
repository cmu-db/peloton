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
  NestedLoopJoinPlan(const NestedLoopJoinPlan &) = delete;
  NestedLoopJoinPlan &operator=(const NestedLoopJoinPlan &) = delete;
  NestedLoopJoinPlan(NestedLoopJoinPlan &&) = delete;
  NestedLoopJoinPlan &operator=(NestedLoopJoinPlan &&) = delete;

  NestedLoopJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema);

  // This is added for passing the left and right join column ids
  NestedLoopJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      std::vector<oid_t> &join_column_ids_left,
      std::vector<oid_t> &join_column_ids_right);

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_NESTLOOP;
  }

  const std::string GetInfo() const { return "NestedLoopJoin"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        GetPredicate()->Copy());

    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));

    NestedLoopJoinPlan *new_plan =
        new NestedLoopJoinPlan(GetJoinType(), std::move(predicate_copy),
                               std::move(GetProjInfo()->Copy()), schema_copy);

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

  // columns in right table for join predicate. This is the physical column id
  std::vector<oid_t> join_column_ids_right_;
};

}  // namespace planner
}  // namespace peloton

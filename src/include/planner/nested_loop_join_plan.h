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
namespace expression{
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

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_NESTLOOP;
  }

  const std::string GetInfo() const { return "NestedLoopJoin"; }

  void SetParameterValues(UNUSED_ATTRIBUTE std::vector<Value>* values) { };

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
};

}  // namespace planner
}  // namespace peloton

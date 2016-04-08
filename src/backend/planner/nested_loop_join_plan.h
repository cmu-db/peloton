//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_plan.h
//
// Identification: src/backend/planner/nested_loop_join_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_join_plan.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/project_info.h"
#include "nodes/plannodes.h"

namespace peloton {
namespace planner {

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
      std::shared_ptr<const catalog::Schema> &proj_schema)
      : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                         proj_schema) {
    nl_ = nullptr;
  }

  NestedLoopJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema, NestLoop *nl)
      : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                         proj_schema) {
    nl_ = nl;
  }  // added to set member nl_

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_NESTLOOP;
  }

  const std::string GetInfo() const { return "NestedLoopJoin"; }

  inline NestLoop *GetNestLoop() const {
    return nl_;
  }  // added to support IN+subquery

  std::unique_ptr<AbstractPlan> Copy() const {
    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        GetPredicate()->Copy());
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));
    NestedLoopJoinPlan *new_plan = new NestedLoopJoinPlan(
        GetJoinType(), std::move(predicate_copy),
        std::move(GetProjInfo()->Copy()), schema_copy, nl_);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  NestLoop *nl_;  // added to support IN+subquery
};

}  // namespace planner
}  // namespace peloton

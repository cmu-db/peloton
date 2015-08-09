//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// merge_join_node.h
//
// Identification: src/backend/planner/merge_join_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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

namespace peloton {
namespace planner {

class MergeJoinPlan : public AbstractJoinPlan {
 public:
  MergeJoinPlan(const MergeJoinPlan &) = delete;
  MergeJoinPlan &operator=(const MergeJoinPlan &) = delete;
  MergeJoinPlan(MergeJoinPlan &&) = delete;
  MergeJoinPlan &operator=(MergeJoinPlan &&) = delete;

  MergeJoinPlan(expression::AbstractExpression *predicate,
                const ProjectInfo *proj_info)
      : AbstractJoinPlan(JOIN_TYPE_INVALID, predicate,
                             proj_info) {  // FIXME
    // Nothing to see here...
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_MERGEJOIN;
  }

  inline std::string GetInfo() const { return "MergeJoin"; }

 private:
  // There is nothing special that we need here
};

}  // namespace planner
}  // namespace peloton

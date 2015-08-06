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

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/project_info.h"
#include "backend/planner/abstract_join_node.h"

namespace peloton {
namespace planner {

class MergeJoinNode : public AbstractJoinPlanNode {
 public:
  MergeJoinNode(const MergeJoinNode &) = delete;
  MergeJoinNode &operator=(const MergeJoinNode &) = delete;
  MergeJoinNode(MergeJoinNode &&) = delete;
  MergeJoinNode &operator=(MergeJoinNode &&) = delete;

  MergeJoinNode(const expression::AbstractExpression *predicate,
                const ProjectInfo *proj_info,
                const expression::AbstractExpression *join_clause)
      : AbstractJoinPlanNode(JOIN_TYPE_INVALID, predicate, proj_info),
        join_clause_(join_clause) {  // FIXME
   // Nothing to see here...
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_MERGEJOIN;
  }

  const inline expression::AbstractExpression *GetJoinClause() const {
    return join_clause_.get();
  }

  inline std::string GetInfo() const { return "MergeJoin"; }

 private:
  std::unique_ptr<const expression::AbstractExpression> join_clause_;
};

}  // namespace planner
}  // namespace peloton

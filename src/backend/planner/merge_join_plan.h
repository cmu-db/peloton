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
<<<<<<< HEAD:src/backend/planner/merge_join_plan.h
  MergeJoinPlan(const MergeJoinPlan &) = delete;
  MergeJoinPlan &operator=(const MergeJoinPlan &) = delete;
  MergeJoinPlan(MergeJoinPlan &&) = delete;
  MergeJoinPlan &operator=(MergeJoinPlan &&) = delete;

  MergeJoinPlan(expression::AbstractExpression *predicate,
                const ProjectInfo *proj_info)
      : AbstractJoinPlan(JOIN_TYPE_INVALID, predicate,
                             proj_info) {  // FIXME
    // Nothing to see here...
=======
  struct JoinClause {
    JoinClause(const expression::AbstractExpression *left,
               const expression::AbstractExpression *right,
               bool reversed)
    : left_(left), right_(right), reversed_(reversed) {}

    JoinClause(const JoinClause &other) = delete;

    JoinClause(JoinClause &&other)
    :left_(std::move(other.left_)),
     right_(std::move(other.right_)),
     reversed_(other.reversed_) {}

    std::unique_ptr<const expression::AbstractExpression> left_;
    std::unique_ptr<const expression::AbstractExpression> right_;
    bool reversed_;
  };

  MergeJoinNode(const MergeJoinNode &) = delete;
  MergeJoinNode &operator=(const MergeJoinNode &) = delete;
  MergeJoinNode(MergeJoinNode &&) = delete;
  MergeJoinNode &operator=(MergeJoinNode &&) = delete;

  MergeJoinNode(const expression::AbstractExpression *predicate,
                const ProjectInfo *proj_info,
                std::vector<JoinClause> &join_clauses)
      : AbstractJoinPlanNode(JOIN_TYPE_INVALID, predicate, proj_info),
        join_clauses_(std::move(join_clauses)) {  // FIXME
   // Nothing to see here...
>>>>>>> bridge:src/backend/planner/merge_join_node.h
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_MERGEJOIN;
  }

  const std::vector<JoinClause> *GetJoinClauses() const {
    return &join_clauses_;
  }

  inline std::string GetInfo() const { return "MergeJoin"; }

 private:
  std::vector<JoinClause> join_clauses_;
};

}  // namespace planner
}  // namespace peloton

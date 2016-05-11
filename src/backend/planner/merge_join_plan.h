//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// merge_join_plan.h
//
// Identification: src/backend/planner/merge_join_plan.h
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

namespace peloton {
namespace planner {

class MergeJoinPlan : public AbstractJoinPlan {
 public:
  struct JoinClause {
    JoinClause(const expression::AbstractExpression *left,
               const expression::AbstractExpression *right, bool reversed)
        : left_(left), right_(right), reversed_(reversed) {}

    JoinClause(const JoinClause &other) = delete;

    JoinClause(JoinClause &&other)
        : left_(std::move(other.left_)),
          right_(std::move(other.right_)),
          reversed_(other.reversed_) {}

    std::unique_ptr<const expression::AbstractExpression> left_;
    std::unique_ptr<const expression::AbstractExpression> right_;
    bool reversed_;
  };

  MergeJoinPlan(const MergeJoinPlan &) = delete;
  MergeJoinPlan &operator=(const MergeJoinPlan &) = delete;
  MergeJoinPlan(MergeJoinPlan &&) = delete;
  MergeJoinPlan &operator=(MergeJoinPlan &&) = delete;

  MergeJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      std::vector<JoinClause> &join_clauses)
      : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                         proj_schema),
        join_clauses_(std::move(join_clauses)) {
    // Nothing to see here...
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_MERGEJOIN;
  }

  const std::vector<JoinClause> *GetJoinClauses() const {
    return &join_clauses_;
  }

  const std::string GetInfo() const { return "MergeJoin"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::vector<JoinClause> new_join_clauses;
    for (size_t i = 0; i < join_clauses_.size(); i++) {
      new_join_clauses.push_back(JoinClause(join_clauses_[i].left_->Copy(),
                                            join_clauses_[i].right_->Copy(),
                                            join_clauses_[i].reversed_));
    }

    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        GetPredicate()->Copy());
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));
    MergeJoinPlan *new_plan = new MergeJoinPlan(
        GetJoinType(), std::move(predicate_copy),
        std::move(GetProjInfo()->Copy()), schema_copy, new_join_clauses);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  std::vector<JoinClause> join_clauses_;
};

}  // namespace planner
}  // namespace peloton

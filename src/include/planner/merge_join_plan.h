//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// merge_join_plan.h
//
// Identification: src/include/planner/merge_join_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_join_plan.h"
#include "common/internal_types.h"
#include "expression/abstract_expression.h"
#include "planner/project_info.h"

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

  MergeJoinPlan(
      JoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      std::vector<JoinClause> &join_clauses)
      : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                         proj_schema),
        join_clauses_(std::move(join_clauses)) {
    // Nothing to see here...
  }

  void HandleSubplanBinding(bool from_left,
                            const BindingContext &input) override {
    for (auto &join_clause : *GetJoinClauses()) {
      auto &exp = from_left ? join_clause.left_ : join_clause.right_;
      const_cast<expression::AbstractExpression *>(exp.get())
          ->PerformBinding({&input});
    }
  }

  inline PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::MERGEJOIN;
  }

  const std::vector<JoinClause> *GetJoinClauses() const {
    return &join_clauses_;
  }

  const std::string GetInfo() const override { return "MergeJoin"; }

  std::unique_ptr<AbstractPlan> Copy() const override {
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
        GetProjInfo()->Copy(), schema_copy, new_join_clauses);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  std::vector<JoinClause> join_clauses_;

 private:
  DISALLOW_COPY_AND_MOVE(MergeJoinPlan);
};

}  // namespace planner
}  // namespace peloton

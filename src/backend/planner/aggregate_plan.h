//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_plan.h
//
// Identification: src/backend/planner/aggregate_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/planner/project_info.h"
#include "backend/common/types.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}

namespace planner {

class AggregatePlan : public AbstractPlan {
 public:
  AggregatePlan() = delete;
  AggregatePlan(const AggregatePlan &) = delete;
  AggregatePlan &operator=(const AggregatePlan &) = delete;
  AggregatePlan(AggregatePlan &&) = delete;

  class AggTerm {
   public:
    ExpressionType aggtype;
    const expression::AbstractExpression *expression;
    bool distinct;

    AggTerm(ExpressionType et, expression::AbstractExpression *expr,
            bool distinct = false)
        : aggtype(et), expression(expr), distinct(distinct) {}

    AggTerm Copy() const {
      return AggTerm(aggtype, expression->Copy(), distinct);
    }
  };

  AggregatePlan(
      std::unique_ptr<const planner::ProjectInfo> &&project_info,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      const std::vector<AggTerm> &&unique_agg_terms,
      const std::vector<oid_t> &&groupby_col_ids,
      std::shared_ptr<const catalog::Schema> &output_schema,
      PelotonAggType aggregate_strategy)
      : project_info_(std::move(project_info)),
        predicate_(std::move(predicate)),
        unique_agg_terms_(unique_agg_terms),
        groupby_col_ids_(groupby_col_ids),
        output_schema_(output_schema),
        agg_strategy_(aggregate_strategy) {}

  const std::vector<oid_t> &GetGroupbyColIds() const {
    return groupby_col_ids_;
  }

  const expression::AbstractExpression *GetPredicate() const {
    return predicate_.get();
  }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  const std::vector<AggTerm> &GetUniqueAggTerms() const {
    return unique_agg_terms_;
  }

  const catalog::Schema *GetOutputSchema() const {
    return output_schema_.get();
  }

  PelotonAggType GetAggregateStrategy() const { return agg_strategy_; }

  inline PlanNodeType GetPlanNodeType() const {
    return PlanNodeType::PLAN_NODE_TYPE_AGGREGATE_V2;
  }

  ~AggregatePlan() {
    for (auto term : unique_agg_terms_) {
      delete term.expression;
    }
  }

  void SetColumnIds(const std::vector<oid_t> &column_ids) {
    column_ids_ = column_ids;
  }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::vector<AggTerm> copied_agg_terms;
    for (const AggTerm &term : unique_agg_terms_) {
      copied_agg_terms.push_back(term.Copy());
    }
    std::vector<oid_t> copied_groupby_col_ids(groupby_col_ids_);

    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        predicate_->Copy());
    std::shared_ptr<const catalog::Schema> output_schema_copy(
        catalog::Schema::CopySchema(GetOutputSchema()));
    AggregatePlan *new_plan = new AggregatePlan(
        std::move(project_info_->Copy()), std::move(predicate_copy),
        std::move(copied_agg_terms), std::move(copied_groupby_col_ids),
        output_schema_copy, agg_strategy_);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  /* For projection */
  std::unique_ptr<const planner::ProjectInfo> project_info_;

  /* For HAVING clause */
  std::unique_ptr<const expression::AbstractExpression> predicate_;

  /* Unique aggregate terms */
  const std::vector<AggTerm> unique_agg_terms_;

  /* Group-by Keys */
  const std::vector<oid_t> groupby_col_ids_;

  /* Output schema */
  std::shared_ptr<const catalog::Schema> output_schema_;

  /* Aggregate Strategy */
  const PelotonAggType agg_strategy_;

  /** @brief Columns involved */
  std::vector<oid_t> column_ids_;
};
}
}

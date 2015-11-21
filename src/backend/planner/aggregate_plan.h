//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// aggregate_node.h
//
// Identification: src/backend/planner/aggregateV2_node.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/planner/abstract_plan.h"
#include "backend/planner/project_info.h"
#include "backend/common/types.h"

namespace peloton {

namespace expression{
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
    const expression::AbstractExpression* expression;
    bool distinct;

    AggTerm(ExpressionType et, expression::AbstractExpression* expr, bool distinct = false)
        : aggtype(et),
          expression(expr),
          distinct(distinct) {
    }
  };

  AggregatePlan(const planner::ProjectInfo* project_info,
                const expression::AbstractExpression* predicate,
                const std::vector<AggTerm>&& unique_agg_terms,
                const std::vector<oid_t>&& groupby_col_ids,
                const catalog::Schema* output_schema,
                PelotonAggType aggregate_strategy)
      : project_info_(project_info),
        predicate_(predicate),
        unique_agg_terms_(unique_agg_terms),
        groupby_col_ids_(groupby_col_ids),
        output_schema_(output_schema),
        agg_strategy_(aggregate_strategy) {

  }

  const std::vector<oid_t>& GetGroupbyColIds() const {
    return groupby_col_ids_;
  }

  const expression::AbstractExpression* GetPredicate() const {
    return predicate_.get();
  }

  const planner::ProjectInfo* GetProjectInfo() const {
    return project_info_.get();
  }

  const std::vector<AggTerm>& GetUniqueAggTerms() const {
    return unique_agg_terms_;
  }

  const catalog::Schema* GetOutputSchema() const {
    return output_schema_.get();
  }

  PelotonAggType GetAggregateStrategy() const {
    return agg_strategy_;
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PlanNodeType::PLAN_NODE_TYPE_AGGREGATE_V2;
  }

  ~AggregatePlan() {
    for (auto term : unique_agg_terms_) {
      delete term.expression;
    }
  }

  void SetColumnIds(const std::vector<oid_t>& column_ids) {
    column_ids_ = column_ids;
  }

  const std::vector<oid_t>& GetColumnIds() const {
    return column_ids_;
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
  std::unique_ptr<const catalog::Schema> output_schema_;

  /* Aggregate Strategy */
  const PelotonAggType agg_strategy_;

  /** @brief Columns involved */
  std::vector<oid_t> column_ids_;

};

}
}

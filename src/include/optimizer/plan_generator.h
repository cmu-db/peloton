//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_to_plan_transformer.h
//
// Identification: src/include/optimizer/operator_to_plan_transformer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"
#include "properties.h"

namespace peloton {

namespace planner {
class AbstractPlan;
class HashJoinPlan;
class NestedLoopJoinPlan;
class ProjectionPlan;
class SeqScanPlan;
class AggregatePlan;
}

namespace optimizer {
class OperatorExpression;
}

namespace optimizer {

class PlanGenerator : public OperatorVisitor {
 public:
  PlanGenerator();

  std::unique_ptr<planner::AbstractPlan> ConvertOpExpression(
      std::shared_ptr<OperatorExpression> op,
      std::shared_ptr<PropertySet> required_props,
      std::vector<expression::AbstractExpression *> required_cols,
      std::vector<expression::AbstractExpression *> output_cols,
      std::vector<std::unique_ptr<planner::AbstractPlan>> &children_plans,
      std::vector<ExprMap> children_expr_map);

  void Visit(const DummyScan *) override;

  void Visit(const PhysicalSeqScan *) override;

  void Visit(const PhysicalIndexScan *) override;

  void Visit(const QueryDerivedScan *) override;

  void Visit(const PhysicalProject *) override;

  void Visit(const PhysicalOrderBy *) override;

  void Visit(const PhysicalLimit *) override;

  void Visit(const PhysicalFilter *) override;

  void Visit(const PhysicalInnerNLJoin *) override;

  void Visit(const PhysicalLeftNLJoin *) override;

  void Visit(const PhysicalRightNLJoin *) override;

  void Visit(const PhysicalOuterNLJoin *) override;

  void Visit(const PhysicalInnerHashJoin *) override;

  void Visit(const PhysicalLeftHashJoin *) override;

  void Visit(const PhysicalRightHashJoin *) override;

  void Visit(const PhysicalOuterHashJoin *) override;

  void Visit(const PhysicalInsert *) override;

  void Visit(const PhysicalInsertSelect *) override;

  void Visit(const PhysicalDelete *) override;

  void Visit(const PhysicalUpdate *) override;

  void Visit(const PhysicalHashGroupBy *) override;

  void Visit(const PhysicalSortGroupBy *) override;

  void Visit(const PhysicalDistinct *) override;

  void Visit(const PhysicalAggregate *) override;

 private:
  ExprMap GenerateTableExprMap(const std::string &alias,
                               const storage::DataTable *table);

  std::vector<oid_t> GenerateColumnsForScan();

  std::unique_ptr<expression::AbstractExpression> GeneratePredicateForScan(
      const std::shared_ptr<expression::AbstractExpression> predicate_expr,
      const std::string &alias, const storage::DataTable *table);

  // Check required columns and output_cols, see if we need to add projection on
  // top of the current output plan, this should be done after the output plan
  // produciing output columns is generated
  void BuildProjectionPlan();
  void BuildAggregatePlan(
      AggregateType aggr_type,
      const std::vector<std::shared_ptr<expression::AbstractExpression>> *
          groupby_cols,
      expression::AbstractExpression *having);
  std::shared_ptr<PropertySet> required_props_;
  std::vector<expression::AbstractExpression *> required_cols_;
  std::vector<expression::AbstractExpression *> output_cols_;
  std::vector<std::unique_ptr<planner::AbstractPlan>> children_plans_;
  std::vector<ExprMap> children_expr_map_;
  std::unique_ptr<planner::AbstractPlan> output_plan_;
};

}  // namespace optimizer
}  // namespace peloton

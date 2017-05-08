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

class OperatorToPlanTransformer : public OperatorVisitor {
 public:
  OperatorToPlanTransformer();

  std::unique_ptr<planner::AbstractPlan> ConvertOpExpression(
      std::shared_ptr<OperatorExpression> plan, PropertySet *requirements,
      std::vector<PropertySet> *required_input_props,
      std::vector<std::unique_ptr<planner::AbstractPlan>> &children_plans,
      std::vector<ExprMap> &children_expr_map, ExprMap *output_expr_map);

  void Visit(const DummyScan *) override;

  void Visit(const PhysicalSeqScan *op) override;

  void Visit(const PhysicalIndexScan *op) override;

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

  void Visit(const PhysicalDelete *) override;

  void Visit(const PhysicalUpdate *) override;

  void Visit(const PhysicalHashGroupBy *) override;

  void Visit(const PhysicalSortGroupBy *) override;

  void Visit(const PhysicalDistinct *) override;

  void Visit(const PhysicalAggregate *) override;

 private:
  void VisitOpExpression(std::shared_ptr<OperatorExpression> op);

  void GenerateTableExprMap(ExprMap &expr_map, const std::string &alias, const storage::DataTable *table);

  std::vector<oid_t> GenerateColumnsForScan(const PropertyColumns *column_prop,
                                            const std::string &alias,
                                            const storage::DataTable *table);

  expression::AbstractExpression *GeneratePredicateForScan(
      const PropertyPredicate *predicate_prop, const std::string &alias, const storage::DataTable *table);

  // Generate group by plan
  std::unique_ptr<planner::AggregatePlan> GenerateAggregatePlan(
      const PropertyColumns *prop_col, AggregateType agg_type,
      const std::vector<std::shared_ptr<expression::AbstractExpression>> *
      group_by_exprs,
      expression::AbstractExpression *having);

  std::unique_ptr<planner::AbstractPlan> GenerateJoinPlan(
      expression::AbstractExpression *join_predicate, JoinType join_type,
      bool is_hash);

  std::unique_ptr<planner::AbstractPlan> output_plan_;
  std::vector<std::unique_ptr<planner::AbstractPlan>> children_plans_;
  PropertySet *requirements_;
  std::vector<PropertySet> *required_input_props_;

  std::vector<ExprMap> children_expr_map_;
  ExprMap *output_expr_map_;
};

} /* namespace optimizer */
} /* namespace peloton */

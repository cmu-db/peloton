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

namespace peloton {

namespace planner {
class AbstractPlan;
class HashJoinPlan;
class NestedLoopJoinPlan;
class ProjectionPlan;
class SeqScanPlan;
class AggregatePlan;
class PropertySet;
}

namespace transaction {
class TransactionContext;
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

  void Visit(const PhysicalOrderBy *) override;

  void Visit(const PhysicalLimit *) override;

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
  /**
   * @brief Generate all tuple value expressions of a base table
   *
   * @param alias Table alias, we used it to construct the tuple value
   *  expression
   * @param table The table object
   *
   * @return a vector of tuple value expression representing column name to
   *  table column id mapping
   */
  std::vector<std::unique_ptr<expression::AbstractExpression>>
  GenerateTableTVExprs(const std::string &alias,
                       std::shared_ptr<catalog::TableCatalogObject> table);

  /**
   * @brief Generate the column oids vector for a scan plan
   *
   * @return a vector of column oid indicating which columns to scan
   */
  std::vector<oid_t> GenerateColumnsForScan();

  /**
   * @brief Generate a predicate expression for scan plans
   *
   * @param predicate_expr the original expression
   * @param alias the table alias
   * @param table the table object
   *
   * @return a predicate that is already evaluated, which could be used to
   *  generate a scan plan i.e. all tuple idx are set
   */
  std::unique_ptr<expression::AbstractExpression> GeneratePredicateForScan(
      const std::shared_ptr<expression::AbstractExpression> predicate_expr,
      const std::string &alias, std::shared_ptr<catalog::TableCatalogObject> table);

  /**
   * @brief Generate projection info and projection schema for join
   *
   * @param proj_info The projection info object
   * @param proj_schema The projection schema object
   */
  void GenerateProjectionForJoin(
      std::unique_ptr<const planner::ProjectInfo> &proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema);

  /**
   * @brief Check required columns and output_cols, see if we need to add
   *  projection on top of the current output plan, this should be done after
   *  the output plan produciing output columns is generated
   */
  void BuildProjectionPlan();
  void BuildAggregatePlan(
      AggregateType aggr_type,
      const std::vector<std::shared_ptr<expression::AbstractExpression>>
          *groupby_cols,
      expression::AbstractExpression *having);

  /**
   * @brief The required output property. Note that we have previously enforced
   *  properties so this is fulfilled by the current operator
   */
  std::shared_ptr<PropertySet> required_props_;
  /**
   * @brief Required columns, this may not be fulfilled by the operator, but we
   *  can always generate a projection if the output column does not fulfill the
   *  requirement
   */
  std::vector<expression::AbstractExpression *> required_cols_;
  /**
   * @brief The output columns, which can be fulfilled by the current operator.
   */
  std::vector<expression::AbstractExpression *> output_cols_;
  std::vector<std::unique_ptr<planner::AbstractPlan>> children_plans_;
  /**
   * @brief The expression maps (expression -> tuple idx)
   */
  std::vector<ExprMap> children_expr_map_;
  /**
   * @brief The final output plan
   */
  std::unique_ptr<planner::AbstractPlan> output_plan_;

  transaction::TransactionContext *txn_;
};

}  // namespace optimizer
}  // namespace peloton

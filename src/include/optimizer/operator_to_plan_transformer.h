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
}

namespace optimizer {
class OperatorExpression;
}

namespace optimizer {

class OperatorToPlanTransformer : public OperatorVisitor {
 public:
  OperatorToPlanTransformer();

  planner::AbstractPlan *ConvertOpExpression(
      std::shared_ptr<OperatorExpression> plan);

  void Visit(const PhysicalScan *op) override;

  void Visit(const PhysicalProject *) override;

  void Visit(const PhysicalComputeExprs *) override;

  void Visit(const PhysicalFilter *) override;

  void Visit(const PhysicalInnerNLJoin *) override;

  void Visit(const PhysicalLeftNLJoin *) override;

  void Visit(const PhysicalRightNLJoin *) override;

  void Visit(const PhysicalOuterNLJoin *) override;

  void Visit(const PhysicalInnerHashJoin *) override;

  void Visit(const PhysicalLeftHashJoin *) override;

  void Visit(const PhysicalRightHashJoin *) override;

  void Visit(const PhysicalOuterHashJoin *) override;

 private:
  void VisitOpExpression(std::shared_ptr<OperatorExpression> op);

  expression::AbstractExpression *ConvertToAbstractExpression(
      UNUSED_ATTRIBUTE std::shared_ptr<OperatorExpression> op);

  catalog::Schema *BuildSchemaFromColumns(std::vector<Column *> columns);

  planner::ProjectInfo *BuildProjectInfoFromExprs(
      std::vector<expression::AbstractExpression *> exprs);

  std::vector<Column *> ConcatLeftAndRightColumns();

  std::unique_ptr<planner::AbstractPlan> output_plan;
  std::vector<std::shared_ptr<OperatorExpression>> current_children;
  std::vector<Column *> output_columns;
  std::vector<Column *> left_columns;
  std::vector<Column *> right_columns;
};

} /* namespace optimizer */
} /* namespace peloton */

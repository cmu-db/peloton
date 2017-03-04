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

  std::unique_ptr<planner::AbstractPlan> ConvertOpExpression(
      std::shared_ptr<OperatorExpression> plan, PropertySet *requirements,
      std::vector<PropertySet> *required_input_props,
      std::vector<std::unique_ptr<planner::AbstractPlan>> &children_plans,
      std::vector<std::vector<std::tuple<oid_t, oid_t, oid_t>>> &
          children_output_columns,
      std::vector<std::tuple<oid_t, oid_t, oid_t>> *output_columns);

  void Visit(const PhysicalScan *op) override;

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

 private:
  void VisitOpExpression(std::shared_ptr<OperatorExpression> op);

  std::unique_ptr<planner::AbstractPlan> output_plan_;
  std::vector<std::unique_ptr<planner::AbstractPlan>> children_plans_;
  PropertySet *requirements_;
  std::vector<PropertySet> *required_input_props_;

  std::vector<std::vector<std::tuple<oid_t, oid_t, oid_t>>>
      children_output_columns_;
  std::vector<std::tuple<oid_t, oid_t, oid_t>> *output_columns_;
};

} /* namespace optimizer */
} /* namespace peloton */

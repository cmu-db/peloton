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
#include "memo.h"

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

class InputColumnDeriver : public OperatorVisitor {
 public:
  InputColumnDeriver();

  std::pair<std::vector<expression::AbstractExpression *>,
            std::vector<std::vector<expression::AbstractExpression *>>>
  DeriveInputColumns(
      GroupExpression *gexpr, std::shared_ptr<PropertySet> properties,
      std::vector<expression::AbstractExpression *> required_cols, Memo *memo);

  void Visit(const DummyScan *) override;

  void Visit(const PhysicalSeqScan *op) override;

  void Visit(const PhysicalIndexScan *op) override;

  void Visit(const QueryDerivedScan *op) override;

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
  // Provide all tuple value expressions needed in the expression
  void ScanHelper();
  void AggregateHelper(const BaseOperatorNode *);
  void JoinHelper(const BaseOperatorNode *op);
  // Some operators, for example limit, directly pass down column property
  void Passdown();
  GroupExpression *gexpr_;
  Memo *memo_;
  /**
   * @brief The derived output columns and input columns, note that the current
   *  operator may have more than one children
   */
  std::pair<std::vector<expression::AbstractExpression *>,
            std::vector<std::vector<expression::AbstractExpression *>>>
      output_input_cols_;
  std::vector<expression::AbstractExpression *> required_cols_;
  std::shared_ptr<PropertySet> properties_;
};

}  // namespace optimizer
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/include/optimizer/cost_calculator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"

namespace peloton {
namespace optimizer {

// Derive cost for a physical group expressionh
class CostCalculator : public OperatorVisitor {
 public:
  double CalculatorCost(
      GroupExpression* gexpr,
      const PropertySet *output_properties);

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
  // We cannot use reference here because otherwise we have to initialize them
  // when constructing the class
  GroupExpression* gexpr_;
  const PropertySet *output_properties_;
  double output_cost_ = 0;
};

}  // namespace optimizer
}  // namespace peloton

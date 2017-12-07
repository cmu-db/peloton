//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/optimizer/cost_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/cost_calculator.h"

namespace peloton {
namespace optimizer {

void CostCalculator::CalculatorCost(std::shared_ptr<GroupExpression> gexpr, const PropertySet *opoutput_properties) {
  gexpr_ = gexpr;
  output_properties_ = output_properties;
  gexpr_->Op().Accept(this);
  return output_cost_;
}

void CostCalculator::Visit(const DummyScan *op) {}
void CostCalculator::Visit(const PhysicalSeqScan *op) {}
void CostCalculator::Visit(const PhysicalIndexScan *op) {}
void CostCalculator::Visit(const QueryDerivedScan *op) {}
void CostCalculator::Visit(const PhysicalProject *op) {}
void CostCalculator::Visit(const PhysicalOrderBy *op) {}
void CostCalculator::Visit(const PhysicalLimit *op) {}
void CostCalculator::Visit(const PhysicalFilter *op) {}
void CostCalculator::Visit(const PhysicalInnerNLJoin *op) {}
void CostCalculator::Visit(const PhysicalLeftNLJoin *op) {}
void CostCalculator::Visit(const PhysicalRightNLJoin *op) {}
void CostCalculator::Visit(const PhysicalOuterNLJoin *op) {}
void CostCalculator::Visit(const PhysicalInnerHashJoin *op) {}
void CostCalculator::Visit(const PhysicalLeftHashJoin *op) {}
void CostCalculator::Visit(const PhysicalRightHashJoin *op) {}
void CostCalculator::Visit(const PhysicalOuterHashJoin *op) {}
void CostCalculator::Visit(const PhysicalInsert *op) {}
void CostCalculator::Visit(const PhysicalInsertSelect *op) {}
void CostCalculator::Visit(const PhysicalDelete *op) {}
void CostCalculator::Visit(const PhysicalUpdate *op) {}
void CostCalculator::Visit(const PhysicalHashGroupBy *op) {}
void CostCalculator::Visit(const PhysicalSortGroupBy *op) {}
void CostCalculator::Visit(const PhysicalDistinct *op) {}
void CostCalculator::Visit(const PhysicalAggregate *op) {}

} // namespace optimizer
} // namespace peloton
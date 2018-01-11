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

double CostCalculator::CalculatorCost(GroupExpression *gexpr, const PropertySet *output_properties) {
  gexpr_ = gexpr;
  output_properties_ = output_properties;
  gexpr_->Op().Accept(this);
  return output_cost_;
}

void CostCalculator::Visit(UNUSED_ATTRIBUTE const DummyScan *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalSeqScan *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalIndexScan *op) {

}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalOrderBy *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalLimit *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInnerNLJoin *op) {
  output_cost_ = 1;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalLeftNLJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalRightNLJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalOuterNLJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInnerHashJoin *op) {
  output_cost_ = 0;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalLeftHashJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalRightHashJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalOuterHashJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInsert *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInsertSelect *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalDelete *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalUpdate *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalHashGroupBy *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalSortGroupBy *op) {
  output_cost_ = 1;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalDistinct *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalAggregate *op) {}

} // namespace optimizer
} // namespace peloton
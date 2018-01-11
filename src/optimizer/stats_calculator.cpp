//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/optimizer/stats_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats_calculator.h"

namespace peloton {
namespace optimizer {

std::shared_ptr<Stats> StatsCalculator::CalculateStats(
    std::shared_ptr<GroupExpression> gexpr) {
  gexpr_ = gexpr;
  gexpr->Op().Accept(this);
  return output_stats_;
}

void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LeafOperator *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalGet *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalQueryDerivedGet *op) {
}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalFilter *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalInnerJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalLeftJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalRightJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalOuterJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalSemiJoin *op) {}
void StatsCalculator::Visit(
    UNUSED_ATTRIBUTE const LogicalAggregateAndGroupBy *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalInsert *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalInsertSelect *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalDelete *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalUpdate *op) {}

}  // namespace optimizer
}  // namespace peloton

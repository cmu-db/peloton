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

void StatsCalculator::CalculateStats(std::shared_ptr<GroupExpression> gexpr) {
  gexpr_ = gexpr;
  gexpr->Op().Accept(this);
  return output_stats_;
}

void StatsCalculator::Visit(const LeafOperator *op) {}
void StatsCalculator::Visit(const LogicalGet *op) {}
void StatsCalculator::Visit(const LogicalQueryDerivedGet *op) {}
void StatsCalculator::Visit(const LogicalFilter *op) {}
void StatsCalculator::Visit(const LogicalInnerJoin *op) {}
void StatsCalculator::Visit(const LogicalLeftJoin *op) {}
void StatsCalculator::Visit(const LogicalRightJoin *op) {}
void StatsCalculator::Visit(const LogicalOuterJoin *op) {}
void StatsCalculator::Visit(const LogicalSemiJoin *op) {}
void StatsCalculator::Visit(const LogicalAggregate *op) {}
void StatsCalculator::Visit(const LogicalGroupBy *op) {}
void StatsCalculator::Visit(const LogicalInsert *op) {}
void StatsCalculator::Visit(const LogicalInsertSelect *op) {}
void StatsCalculator::Visit(const LogicalDelete *op) {}
void StatsCalculator::Visit(const LogicalUpdate *op) {}


} // namespace optimizer
} // namespace peloton
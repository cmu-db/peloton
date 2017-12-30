//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/include/optimizer/stats_calculator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"

namespace peloton {
namespace optimizer {

// Derive stats for a logical group expression
class StatsCalculator : public OperatorVisitor {
 public:
  std::shared_ptr<Stats> CalculateStats(std::shared_ptr<GroupExpression> gexpr);

  void Visit(const LeafOperator *) override;
  void Visit(const LogicalGet *) override;
  void Visit(const LogicalQueryDerivedGet *) override;
  void Visit(const LogicalFilter *) override;
  void Visit(const LogicalInnerJoin *) override;
  void Visit(const LogicalLeftJoin *) override;
  void Visit(const LogicalRightJoin *) override;
  void Visit(const LogicalOuterJoin *) override;
  void Visit(const LogicalSemiJoin *) override;
  void Visit(const LogicalAggregateAndGroupBy *) override;
  void Visit(const LogicalInsert *) override;
  void Visit(const LogicalInsertSelect *) override;
  void Visit(const LogicalDelete *) override;
  void Visit(const LogicalUpdate *) override;

 private:
  // We cannot use reference here because otherwise we have to initialize them
  // when constructing the class
  std::shared_ptr<GroupExpression> gexpr_;

  std::shared_ptr<Stats> output_stats_;
};

}  // namespace optimizer
}  // namespace peloton

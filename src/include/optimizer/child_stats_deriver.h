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

namespace expression {
class AbstractExpression;
}
namespace optimizer {

class Memo;

// Derive child stats that has not yet been calculated for a logical group
// expression
class ChildStatsDeriver : public OperatorVisitor {
 public:
  std::vector<ExprSet> DeriveInputStats(
      GroupExpression *gexpr,
      ExprSet required_cols, Memo *memo);

  void Visit(const LogicalQueryDerivedGet *) override;
  void Visit(const LogicalInnerJoin *) override;
  void Visit(const LogicalLeftJoin *) override;
  void Visit(const LogicalRightJoin *) override;
  void Visit(const LogicalOuterJoin *) override;
  void Visit(const LogicalSemiJoin *) override;
  void Visit(const LogicalAggregateAndGroupBy *) override;

 private:
  void PassDownRequiredCols();
  void PassDownColumn(expression::AbstractExpression* col);
  ExprSet required_cols_;
  GroupExpression *gexpr_;
  Memo *memo_;

  std::vector<ExprSet> output_;
};

}  // namespace optimizer
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_cost_calculator.h
//
// Identification: src/include/optimizer/abstract_cost_calculator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"
#include "optimizer/operator_expression.h"

namespace peloton {
namespace optimizer {

template <class Node, class OperatorType, class OperatorExpr>
class Memo;

// Default cost when cost model cannot compute correct cost.
static constexpr double DEFAULT_COST = 1;

// Estimate the cost of processing each row during a query.
static constexpr double DEFAULT_TUPLE_COST = 0.01;

// Estimate the cost of processing each index entry during an index scan.
static constexpr double DEFAULT_INDEX_TUPLE_COST = 0.005;

// Estimate the cost of processing each operator or function executed during a
// query.
static constexpr double DEFAULT_OPERATOR_COST = 0.0025;

class AbstractCostModel : public OperatorVisitor {
 public:
  virtual double CalculateCost(GroupExpression<Operator,OpType,OperatorExpression> *gexpr,
                               Memo<Operator,OpType,OperatorExpression> *memo,
                               concurrency::TransactionContext *txn) = 0;
};

}  // namespace optimizer
}  // namespace peloton


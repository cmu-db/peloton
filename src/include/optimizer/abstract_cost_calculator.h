//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_cost_calculator.h
//
// Identification: src/include/optimizer/abstract_cost_calculator.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"

namespace peloton {
namespace optimizer {

class Memo;

class AbstractCostCalculator : public OperatorVisitor {
 public:
  virtual double CalculateCost(GroupExpression *gexpr, Memo *memo,
                               concurrency::TransactionContext *txn) = 0;
};

}  // namespace optimizer
}  // namespace peloton
//
// Created by nate on 4/8/18.
//

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
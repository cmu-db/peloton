//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// op_expression.cpp
//
// Identification: src/optimizer/op_expression.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/operator_expression.h"

#include <limits>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Expression
//===--------------------------------------------------------------------===//
OperatorExpression::OperatorExpression(Operator op) : op(op) {}

void OperatorExpression::PushChild(std::shared_ptr<OperatorExpression> op) {
  children.push_back(op);
}

void OperatorExpression::PopChild() { children.pop_back(); }

const std::vector<std::shared_ptr<OperatorExpression>> &
OperatorExpression::Children() const {
  return children;
}

const Operator &OperatorExpression::Op() const { return op; }

}  // namespace optimizer
}  // namespace peloton

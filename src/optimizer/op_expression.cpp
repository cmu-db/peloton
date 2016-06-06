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

#include "optimizer/op_expression.h"

#include <limits>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Expression
//===--------------------------------------------------------------------===//
OpExpression::OpExpression(Operator op) : op(op) {}

void OpExpression::PushChild(std::shared_ptr<OpExpression> op) {
  children.push_back(op);
}

void OpExpression::PopChild() { children.pop_back(); }

const std::vector<std::shared_ptr<OpExpression>> &OpExpression::Children()
    const {
  return children;
}

const Operator &OpExpression::Op() const { return op; }

} /* namespace optimizer */
} /* namespace peloton */

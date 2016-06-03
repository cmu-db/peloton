//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// op_expression.h
//
// Identification: src/backend/optimizer/op_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/operator_node.h"
#include "backend/optimizer/group.h"

#include <vector>
#include <memory>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Expr
//===--------------------------------------------------------------------===//
class OpExpressionVisitor;

class OpExpression {
public:
  OpExpression(Operator op);

  void PushChild(std::shared_ptr<OpExpression> op);

  void PopChild();

  const std::vector<std::shared_ptr<OpExpression>> &Children() const;

  const Operator &Op() const;

private:
  Operator op;
  std::vector<std::shared_ptr<OpExpression>> children;
};

} /* namespace optimizer */
} /* namespace peloton */

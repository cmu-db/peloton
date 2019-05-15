//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.h
//
// Identification: src/include/optimizer/operator_expression.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/abstract_node_expression.h"
#include "optimizer/operator_node.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Expression
//===--------------------------------------------------------------------===//
class OperatorExpression : public AbstractNodeExpression {
 public:
  OperatorExpression(std::shared_ptr<AbstractNode> node);

  void PushChild(std::shared_ptr<AbstractNodeExpression> child);

  void PopChild();

  const std::vector<std::shared_ptr<AbstractNodeExpression>> &Children() const;

  const std::shared_ptr<AbstractNode> Node() const;

  const std::string GetInfo() const;

 private:
  std::shared_ptr<AbstractNode> node;
  std::vector<std::shared_ptr<AbstractNodeExpression>> children;
};

}  // namespace optimizer
}  // namespace peloton

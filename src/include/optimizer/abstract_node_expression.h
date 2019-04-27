//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_node_expression.h
//
// Identification: src/include/optimizer/abstract_node_expression.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/abstract_node.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Abstract Node Expression
//===--------------------------------------------------------------------===//
class AbstractNodeExpression {
 public:
  AbstractNodeExpression() {}

  ~AbstractNodeExpression() {}

  virtual void PushChild(std::shared_ptr<AbstractNodeExpression> child) = 0;

  virtual void PopChild() = 0;

  virtual const std::vector<std::shared_ptr<AbstractNodeExpression>> &Children() const = 0;

  virtual const std::shared_ptr<AbstractNode> Node() const = 0;

  virtual const std::string GetInfo() const = 0;
};

}  // namespace optimizer
}  // namespace peloton

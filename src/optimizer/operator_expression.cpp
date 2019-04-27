//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.cpp
//
// Identification: src/optimizer/operator_expression.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/operator_expression.h"

#include <limits>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Expression
//===--------------------------------------------------------------------===//
OperatorExpression::OperatorExpression(std::shared_ptr<AbstractNode> node) : node(node) {}

void OperatorExpression::PushChild(std::shared_ptr<AbstractNodeExpression> node) {
  children.push_back(node);
}

void OperatorExpression::PopChild() { children.pop_back(); }

const std::vector<std::shared_ptr<AbstractNodeExpression>>
    &OperatorExpression::Children() const {
  return children;
}

const std::shared_ptr<AbstractNode> OperatorExpression::Node() const { return node; }

const std::string OperatorExpression::GetInfo() const {
  std::string info = "{";
  {
    info += "\"Op\":";
    info += "\"" + node->GetName() + "\",";
    if (!children.empty()) {
      info += "\"Children\":[";
      {
        bool is_first = true;
        for (const auto &child : children) {
          if (is_first == true) {
            is_first = false;
          } else {
            info += ",";
          }
          info += child->GetInfo();
        }
      }
      info += "]";
    }
  }
  info += '}';
  return info;
}

}  // namespace optimizer
}  // namespace peloton

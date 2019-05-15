//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pattern.cpp
//
// Identification: src/optimizer/pattern.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/pattern.h"

namespace peloton {
namespace optimizer {

Pattern::Pattern(OpType op) : _op_type(op) {}
Pattern::Pattern(ExpressionType exp) : _exp_type(exp) {}

void Pattern::AddChild(std::shared_ptr<Pattern> child) {
  children.push_back(child);
}

const std::vector<std::shared_ptr<Pattern>> &Pattern::Children() const {
  return children;
}

OpType Pattern::GetOpType() const { return _op_type; }
ExpressionType Pattern::GetExpType() const { return _exp_type; }

}  // namespace optimizer
}  // namespace peloton

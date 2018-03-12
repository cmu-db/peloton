//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_node.cpp
//
// Identification: src/optimizer/operator_node.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/operator_node.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
Operator::Operator() : node(nullptr) {}

Operator::Operator(BaseOperatorNode *node) : node(node) {}

void Operator::Accept(OperatorVisitor *v) const { node->Accept(v); }

std::string Operator::GetName() const {
  if (IsDefined()) {
    return node->GetName();
  }
  return "Undefined";
}

OpType Operator::GetType() const {
  if (IsDefined()) {
    return node->GetType();
  }
  return OpType::Undefined;
}

bool Operator::IsLogical() const {
  if (IsDefined()) {
    return node->IsLogical();
  }
  return false;
}

bool Operator::IsPhysical() const {
  if (IsDefined()) {
    return node->IsPhysical();
  }
  return false;
}

hash_t Operator::Hash() const {
  if (IsDefined()) {
    return node->Hash();
  }
  return 0;
}

bool Operator::operator==(const Operator &r) {
  if (IsDefined() && r.IsDefined()) {
    return *node == *r.node;
  } else if (!IsDefined() && !r.IsDefined()) {
    return true;
  }
  return false;
}

bool Operator::IsDefined() const { return node != nullptr; }

}  // namespace optimizer
}  // namespace peloton

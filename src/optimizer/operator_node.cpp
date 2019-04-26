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

#include "optimizer/abstract_node.h"
#include "optimizer/operator_node.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
Operator::Operator() : AbstractNode(nullptr) {}

Operator::Operator(AbstractNode *node) : AbstractNode(node) {}

void Operator::Accept(OperatorVisitor *v) const { node->Accept(v); }

std::string Operator::GetName() const {
  if (IsDefined()) {
    return node->GetName();
  }
  return "Undefined";
}

OpType Operator::GetOpType() const {
  if (IsDefined()) {
    return node->GetOpType();
  }
  return OpType::Undefined;
}

ExpressionType Operator::GetExpType() const {
  if (IsDefined()) {
    return node->GetExpType();
  }
  return ExpressionType::INVALID;
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

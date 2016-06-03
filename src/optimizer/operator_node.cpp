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

Operator::Operator(BaseOperatorNode* node) : node(node) {}

void Operator::accept(OperatorVisitor *v) const {
  node->accept(v);
}

std::string Operator::name() const {
  if (defined()) {
    return node->name();
  }
  return "Undefined";
}

OpType Operator::type() const {
  if (defined()) {
    return node->type();
  }
  return OpType::Undefined;
}

bool Operator::IsLogical() const {
  if (defined()) {
    return node->IsLogical();
  }
  return false;
}

bool Operator::IsPhysical() const {
  if (defined()) {
    return node->IsPhysical();
  }
  return false;
}

std::vector<PropertySet> Operator::RequiredInputProperties() const {
  if (defined()) {
    return node->RequiredInputProperties();
  }
  return {};
}

PropertySet Operator::ProvidedOutputProperties() const {
  if (defined()) {
    return node->ProvidedOutputProperties();
  }
  return PropertySet();
}

hash_t Operator::Hash() const {
  if (defined()) {
    return node->Hash();
  }
  return 0;
}

bool Operator::operator==(const Operator &r) {
  if (defined() && r.defined()) {
    return *node == *r.node;
  } else if (!defined() && !r.defined()) {
    return true;
  }
  return false;
}

bool Operator::defined() const {
  return node != nullptr;
}

} /* namespace optimizer */
} /* namespace peloton */

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

template <class OperatorType>
Pattern<OperatorType>::Pattern(OperatorType op) : _type(op) {}

template <class OperatorType>
void Pattern<OperatorType>::AddChild(std::shared_ptr<Pattern<OperatorType>> child) {
  children.push_back(child);
}

template <class OperatorType>
const std::vector<std::shared_ptr<Pattern<OperatorType>>> &Pattern<OperatorType>::Children() const {
  return children;
}

template <class OperatorType>
OperatorType Pattern<OperatorType>::Type() const { return _type; }

// Explicitly instantiate
template class Pattern<OpType>;

}  // namespace optimizer
}  // namespace peloton

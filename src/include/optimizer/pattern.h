//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pattern.h
//
// Identification: src/include/optimizer/pattern.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>

#include "optimizer/operator_node.h"

namespace peloton {
namespace optimizer {

template <typename T>
class PatternBase {
 public:
  PatternBase(T op) : _type(op) {}

  void AddChild(std::shared_ptr<PatternBase> child) {
    children.push_back(child);
  }

  const std::vector<std::shared_ptr<PatternBase>> &Children() const {
    return children;
  }

  inline size_t GetChildPatternsSize() const { return children.size(); }

  T Type() const { return _type; }

 protected:
  T _type;

  std::vector<std::shared_ptr<PatternBase>> children;
};

using Pattern = PatternBase<OpType>;

}  // namespace optimizer
}  // namespace peloton

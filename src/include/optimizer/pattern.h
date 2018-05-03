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

class Pattern {
 public:
  Pattern(OpType op);

  void AddChild(std::shared_ptr<Pattern> child);

  const std::vector<std::shared_ptr<Pattern>> &Children() const;

  inline size_t GetChildPatternsSize() const { return children.size(); }

  OpType Type() const;

 private:
  OpType _type;
  std::vector<std::shared_ptr<Pattern>> children;
};

}  // namespace optimizer
}  // namespace peloton

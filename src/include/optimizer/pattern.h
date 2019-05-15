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

  Pattern(ExpressionType exp_type);

  void AddChild(std::shared_ptr<Pattern> child);

  const std::vector<std::shared_ptr<Pattern>> &Children() const;

  inline size_t GetChildPatternsSize() const { return children.size(); }

  OpType GetOpType() const;

  ExpressionType GetExpType() const;

 private:
  OpType _op_type = OpType::Undefined;
  ExpressionType _exp_type = ExpressionType::INVALID;
  std::vector<std::shared_ptr<Pattern>> children;
};

}  // namespace optimizer
}  // namespace peloton

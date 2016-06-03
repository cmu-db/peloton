//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pattern.h
//
// Identification: src/backend/optimizer/pattern.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/operator_node.h"

#include <vector>
#include <memory>

namespace peloton {
namespace optimizer {

class Pattern {
 public:
  Pattern(OpType op);

  void AddChild(std::shared_ptr<Pattern> child);

  const std::vector<std::shared_ptr<Pattern>> &Children() const;

  OpType Type() const;

 private:
  OpType _type;
  std::vector<std::shared_ptr<Pattern>> children;
};

} /* namespace optimizer */
} /* namespace peloton */

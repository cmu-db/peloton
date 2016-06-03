//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pattern.cpp
//
// Identification: src/backend/optimizer/pattern.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/optimizer/pattern.h"

namespace peloton {
namespace optimizer {

Pattern::Pattern(OpType op)
  : _type(op)
{
}

void Pattern::AddChild(std::shared_ptr<Pattern> child) {
  children.push_back(child);
}

const std::vector<std::shared_ptr<Pattern>> &Pattern::Children() const {
  return children;
}

OpType Pattern::Type() const {
  return _type;
}

} /* namespace optimizer */
} /* namespace peloton */

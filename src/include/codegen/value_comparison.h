//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_comparison.h
//
// Identification: src/include/codegen/value_comparison.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Handy class that helps generate code for value equality
//===----------------------------------------------------------------------===//
class ValueComparison {
 public:
  // This method generates code that checks the equality of every value in lhs
  // with its corresponding value in rhs.
  static codegen::Value TestEquality(CodeGen &codegen,
                                     const std::vector<codegen::Value> &lhs,
                                     const std::vector<codegen::Value> &rhs);
};

}  // namespace codegen
}  // namespace peloton
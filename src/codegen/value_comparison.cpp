//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_comparison.cpp
//
// Identification: src/codegen/value_comparison.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/value_comparison.h"

#include <queue>
#include <list>

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return whether every value on the left-hand-side is equivalent to its
// counterpart on the right-hand-side.
//===----------------------------------------------------------------------===//
codegen::Value ValueComparison::TestEquality(
    CodeGen &codegen, const std::vector<codegen::Value> &lhs,
    const std::vector<codegen::Value> &rhs) {
  std::queue<codegen::Value, std::list<codegen::Value>> results;
  // Perform the comparison of each element of lhs to rhs
  for (size_t i = 0; i < lhs.size(); i++) {
    results.push(lhs[i].CompareEq(codegen, rhs[i]));
  }

  // Tournament-style collapse
  while (results.size() > 1) {
    codegen::Value first = results.front();
    results.pop();
    codegen::Value second = results.front();
    results.pop();
    results.push(first.LogicalAnd(codegen, second));
  }
  return results.front();
}

}  // namespace codegen
}  // namespace peloton
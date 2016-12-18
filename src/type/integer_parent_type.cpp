//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_value.cpp
//
// Identification: src/backend/common/numeric_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/integer_parent_type.h"

#include <cmath>
#include <iostream>
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/varlen_type.h"

namespace peloton {
namespace common {

IntegerParentType::IntegerParentType(TypeId type) : NumericType(type) {}

Value IntegerParentType::Min(const Value& left, const Value& right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);

  Value cmp = (left.CompareLessThan(right));
  if (cmp.IsTrue()) return left.Copy();
  return right.Copy();
}

Value IntegerParentType::Max(const Value& left, const Value& right) const {
  left.CheckInteger();
  left.CheckComparable(right);
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);

  Value cmp = (left.CompareGreaterThanEquals(right));
  if (cmp.IsTrue()) return left.Copy();
  return right.Copy();
}

}  // namespace common
}  // namespace peloton

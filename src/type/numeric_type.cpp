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

#include "type/numeric_type.h"

#include <cmath>
#include <iostream>
#include "type/boolean_type.h"
#include "type/decimal_type.h"
#include "type/varlen_type.h"

namespace peloton {
namespace type {

NumericType::~NumericType() {}

}  // namespace type
}  // namespace peloton

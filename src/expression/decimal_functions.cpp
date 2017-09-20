//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions.cpp
//
// Identification: src/expression/decimal_functions.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "expression/abstract_expression.h"
#include "expression/decimal_functions.h"
#include "type/value_factory.h"

namespace peloton {
namespace expression {
// Get square root of the value
type::Value DecimalFunctions::Sqrt(const std::vector<type::Value>& args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return args[0].Sqrt();
}
}
}

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_functions.cpp
//
// Identification: src/function/decimal_functions.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/decimal_functions.h"
#include "type/value_factory.h"

namespace peloton {
namespace function {

// Get square root of the value
type::Value DecimalFunctions::Sqrt(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return args[0].Sqrt();
}

// Get absolute value of the value
double DecimalFunctions::Abs(double arg) { return fabs(arg); }

type::Value DecimalFunctions::_Abs(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  double res = Abs(args[0].GetAs<double>());
  return type::ValueFactory::GetDecimalValue(res);
}

}  // namespace function
}  // namespace peloton

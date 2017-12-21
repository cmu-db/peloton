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


// Get ceiling of value
type::Value DecimalFunctions::_Ceil(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  double result;
  switch (args[0].GetElementType()) {
    case type::TypeId::DECIMAL:
      result = Ceil(args[0].GetAs<double>());
      break;
    case type::TypeId::INTEGER:
      result = args[0].GetAs<int32_t>();
      break;
    case type::TypeId::BIGINT:
      result = args[0].GetAs<int64_t>();
      break;
    case type::TypeId::SMALLINT:
      result = args[0].GetAs<int16_t>();
      break;
    case type::TypeId::TINYINT:
      result = args[0].GetAs<int8_t>();
      break;
    default:
      return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return type::ValueFactory::GetDecimalValue(result);
}

double DecimalFunctions::Ceil(const double args) { return ceil(args); }

// Get floor value
type::Value DecimalFunctions::_Floor(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  double res;
  switch(args[0].GetElementType()) {
    case type::TypeId::DECIMAL:
      res = Floor(args[0].GetAs<double>());
      break;
    case type::TypeId::INTEGER:
      res = args[0].GetAs<int32_t>();
      break;
    case type::TypeId::BIGINT:
      res = args[0].GetAs<int64_t>();
      break;
    case type::TypeId::SMALLINT:
      res = args[0].GetAs<int16_t>();
      break;
    case type::TypeId::TINYINT:
      res = args[0].GetAs<int8_t>();
      break;
    default:
      return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return type::ValueFactory::GetDecimalValue(res);
}

double DecimalFunctions::Floor(const double val) { return floor(val); }

// Round to nearest integer
type::Value DecimalFunctions::_Round(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  return type::ValueFactory::GetDecimalValue(Round(args[0].GetAs<double>()));
}

double DecimalFunctions::Round(double arg) { return round(arg); }


}  // namespace function
}  // namespace peloton

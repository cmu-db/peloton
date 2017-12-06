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
#include <math.h>
#include <limits>

namespace peloton {
namespace function {

double DecimalFunctions::Sqrt(double arg) {
  try {
    if (arg < 0) {
      throw CatalogException("cannot take square root of a negative number");
    }
  } catch (CatalogException &e) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  return sqrt(arg);
}

double DecimalFunctions::SqrtInt(int64_t arg) {
  return Sqrt(static_cast<double>(arg));
}

// Get square root of the value
type::Value DecimalFunctions::_Sqrt(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  LOG_INFO("GetTypeId() = %d", args[0].GetTypeId());
  double ret = 0.0;
  switch (args[0].GetTypeId()) {
    case peloton::type::TypeId::DECIMAL:
      ret = Sqrt(args[0].GetAs<double>());
      break;
    case peloton::type::TypeId::TINYINT:
      ret = Sqrt(static_cast<int64_t>(args[0].GetAs<int8_t>()));
      break;
    case peloton::type::TypeId::SMALLINT:
      ret = Sqrt(static_cast<int64_t>(args[0].GetAs<int16_t>()));
      break;
    case peloton::type::TypeId::INTEGER:
      ret = Sqrt(static_cast<int64_t>(args[0].GetAs<int32_t>()));
      break;
    case peloton::type::TypeId::BIGINT:
      ret = Sqrt(args[0].GetAs<int64_t>());
      break;
    default:
      LOG_ERROR("INVALID TYPE. The id type is %d", args[0].GetTypeId());
      throw CatalogException("INVALID TYPE. Expect a decimal or integers");
  };
  LOG_INFO("The result is %lf", ret);
  auto result = type::ValueFactory::GetDecimalValue(ret);
  LOG_INFO("ValueFactory Result: %s", result.GetInfo().c_str());
  return result;
}

// Get floor value
type::Value DecimalFunctions::_Floor(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }
  double res;
  switch (args[0].GetElementType()) {
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

}  // namespace function
}  // namespace peloton

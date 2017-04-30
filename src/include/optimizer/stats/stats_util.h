//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_util.h
//
// Identification: src/include/optimizer/stats/stats_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace optimizer {

// Convert numeric peloton value type to primitive value.
double PelotonValueToNumericValue(const type::Value& value) {
  double raw_value = 0;
  if (value.IsNull()) {
    LOG_TRACE("Fail to convert Peloton NULL value to numeric value.");
    return raw_value;
  }
  if (value.CheckInteger() || value.GetTypeId() == type::Type::TIMESTAMP) {
    raw_value = static_cast<double>(value.GetAs<int>());
  } else if (value.GetTypeId() == type::Type::DECIMAL) {
    raw_value = value.GetAs<double>();
  } else {
    LOG_TRACE("Fail to convert non-numeric Peloton value to numeric value");
  }
  return raw_value;
}

} /* namespace optimizer */
} /* namespace peloton */

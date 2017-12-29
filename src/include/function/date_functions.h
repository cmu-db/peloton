//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions.h
//
// Identification: src/include/function/date_functions.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "common/logger.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {
namespace function {

class DateFunctions {
 public:
  // The arguments are contained in the args vector
  // (1) The first argument is the part of the date to extract
  // (see DatePartType in type/types.h
  // (2) The second argument is the timestamp to extract the part from
  // @return The Value returned should be a type::DecimalValue that is
  // constructed using type::ValueFactory
  static type::Value Extract(const std::vector<type::Value> &args);

  static int64_t Now();
  static type::Value _Now(const std::vector<type::Value> &args);
};

}  // namespace function
}  // namespace peloton

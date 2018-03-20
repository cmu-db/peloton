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
  static int64_t Now();
  static type::Value _Now(const std::vector<type::Value> &args);
};

}  // namespace function
}  // namespace peloton

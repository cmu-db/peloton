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

namespace codegen {
namespace type {
class Type;
}  // namespace type
}  // namespace codegen

namespace function {

class DateFunctions {
 public:
  /**
   * Function used to return the current date/time. Normally called at the start
   * of a transaction, and consistent throughout its duration.
   *
   * @return The current date at the time of invocation
   */
  static int64_t Now();
  static type::Value _Now(const std::vector<type::Value> &args);

  /**
   *
   * @param data
   * @param len
   * @return
   */
  static int32_t InputDate(const codegen::type::Type &type, const char *data,
                           uint32_t len);
};

}  // namespace function
}  // namespace peloton

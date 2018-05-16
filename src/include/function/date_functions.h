//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions.h
//
// Identification: src/include/function/date_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>

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
   * Convert the given input into a Julian date format.
   *
   * @param year The year
   * @param month The month (1-based)
   * @param day The day (1-based)
   * @return The equivalent 32-bit integer representation of the date
   */
  static int32_t DateToJulian(int32_t year, int32_t month, int32_t day);

  /**
   * Decompose the given 32-bit Julian date value into year, month, and day
   * components.
   *
   * @param julian_date The julian date
   * @param year[out] Where the year is written
   * @param month[out] Where the result month is written
   * @param day[out] Where the result day is written
   */
  static void JulianToDate(int32_t julian_date, int32_t &year, int32_t &month,
                           int32_t &day);

  /**
   * Convert the given input string into a date.
   *
   * @param data A pointer to a string representation of a date
   * @param len The length of the string
   * @return A suitable date representation of the given input string that can
   * be stored in the data tables. This typically means a Julian date.
   */
  static int32_t InputDate(const codegen::type::Type &type, const char *data,
                           uint32_t len);
};

}  // namespace function
}  // namespace peloton

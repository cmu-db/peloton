//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions.cpp
//
// Identification: src/function/timestamp_functions.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/timestamp_functions.h"

#include <date/date.h>
#include <date/iso_week.h>
#include <inttypes.h>

#include "common/logger.h"
#include "type/limits.h"
#include "type/value_factory.h"

namespace peloton {
namespace function {

uint64_t TimestampFunctions::DateTrunc(uint32_t date_part_type,
                                       uint64_t value) {
  DatePartType date_part = static_cast<DatePartType>(date_part_type);

  uint64_t timestamp = value;
  uint64_t result = 0;

  // TODO(lma): A more efficient NULL check is to implement something like the
  // value::CallBinaryOp() for arithmetic operations. Right now the timestamp
  // functions doesn't have that infrastructure.
  if (timestamp == type::PELOTON_TIMESTAMP_NULL) {
    return type::PELOTON_TIMESTAMP_NULL;
  }

  uint32_t micro = timestamp % 1000000;
  timestamp /= 1000000;
  uint32_t hour_min_sec = timestamp % 100000;
  uint16_t sec = hour_min_sec % 60;
  hour_min_sec /= 60;
  hour_min_sec /= 60;
  uint16_t hour = hour_min_sec % 24;
  timestamp /= 100000;
  uint16_t year = timestamp % 10000;
  timestamp /= 10000;
  uint16_t tz = timestamp % 27;
  timestamp /= 27;  // skip time zone
  uint16_t day = timestamp % 32;
  timestamp /= 32;
  uint16_t month = timestamp;

  uint8_t quarter = (month - 1) / 3 + 1;

  date::year_month_day ymd = date::year_month_day{
      date::year{year}, date::month{month}, date::day{day}};
  iso_week::year_weeknum_weekday yww = iso_week::year_weeknum_weekday{ymd};

  uint16_t dow = ((unsigned)yww.weekday()) == 7 ? 0 : (unsigned)yww.weekday();

  switch (date_part) {
    case DatePartType::CENTURY: {
      result =
          ((((uint64_t)32 + 1) * 27 + tz) * 10000 + year - year % 100 + 1) *
          100000 * 1000000;
      break;
    }
    case DatePartType::DAY: {
      result = ((((uint64_t)month * 32 + day) * 27 + tz) * 10000 + year) *
               100000 * 1000000;
      break;
    }
    case DatePartType::DECADE: {
      result = ((((uint64_t)32 + 1) * 27 + tz) * 10000 + year - year % 10) *
               100000 * 1000000;
      break;
    }
    case DatePartType::HOUR: {
      result =
          (((((uint64_t)month * 32 + day) * 27 + tz) * 10000 + year) * 100000 +
           hour * 3600) *
          1000000;
      break;
    }
    case DatePartType::MICROSECOND: {
      result = value;
      break;
    }
    case DatePartType::MILLENNIUM: {
      result =
          ((((uint64_t)32 + 1) * 27 + tz) * 10000 + year - year % 1000 + 1) *
          100000 * 1000000;
      break;
    }
    case DatePartType::MILLISECOND: {
      result = value - micro % 1000;
      break;
    }
    case DatePartType::MINUTE: {
      result = value - micro - sec * 1000000;
      break;
    }
    case DatePartType::MONTH: {
      result = ((((uint64_t)month * 32 + 1) * 27 + tz) * 10000 + year) *
               100000 * 1000000;
      break;
    }
    case DatePartType::QUARTER: {
      result =
          ((((((uint64_t)quarter - 1) * 3 + 1) * 32 + 1) * 27 + tz) * 10000 +
           year) *
          100000 * 1000000;
      break;
    }
    case DatePartType::SECOND: {
      result = value - micro;
      break;
    }
    case DatePartType::WEEK: {
      result =
          ((((uint64_t)month * 32 + day - (dow == 0 ? 6 : (dow - 1))) * 27 +
            tz) *
               10000 +
           year) *
          100000 * 1000000;
      break;
    }
    case DatePartType::YEAR: {
      result =
          ((((uint64_t)32 + 1) * 27 + tz) * 10000 + year) * 100000 * 1000000;
      break;
    }
    default: { result = type::PELOTON_TIMESTAMP_NULL; }
  };

  return (result);
}

type::Value TimestampFunctions::_DateTrunc(
    const std::vector<type::Value> &args) {
  uint32_t date_part = args[0].GetAs<uint32_t>();
  uint64_t timestamp = args[1].GetAs<uint64_t>();
  type::Value result;

  if (timestamp == type::PELOTON_TIMESTAMP_NULL) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP);
  }

  uint64_t ret = DateTrunc(date_part, timestamp);
  return type::ValueFactory::GetTimestampValue(ret);
}
}  // namespace function
}  // namespace peloton

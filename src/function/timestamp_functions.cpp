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
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace function {

uint64_t TimestampFunctions::DateTrunc(uint32_t date_part_type,
                                       uint64_t value) {
  DatePartType date_part =
      *reinterpret_cast<const DatePartType*>(&date_part_type);

  uint64_t timestamp = value;
  uint64_t result = 0;

  if (timestamp == type::PELOTON_TIMESTAMP_NULL) {
    return type::PELOTON_TIMESTAMP_NULL;
  }

  uint32_t micro = timestamp % 1000000;
  timestamp /= 1000000;
  uint32_t hour_min_sec = timestamp % 100000;
  hour_min_sec /= 60;
  uint16_t min = hour_min_sec % 60;
  hour_min_sec /= 60;
  uint16_t hour = hour_min_sec % 24;
  timestamp /= 100000;
  uint16_t year = timestamp % 10000;
  timestamp /= 10000;
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
      result = ((32 + 1) * 27 * 10000 + year - year % 100) * 100000 * 1000000;
      break;
    }
    case DatePartType::DAY: {
      result = ((month * 32 + day) * 27 * 10000 + year) * 100000 * 1000000;
      break;
    }
    case DatePartType::DECADE: {
      result = ((32 + 1) * 27 * 10000 + year - year % 10) * 100000 * 1000000;
      break;
    }
    case DatePartType::HOUR: {
      result = (((month * 32 + day) * 27 * 10000 + year - year % 100) * 100000 +
                hour * 86400) *
               1000000;
      break;
    }
    case DatePartType::MICROSECOND: {
      result = value;
      break;
    }
    case DatePartType::MILLENNIUM: {
      result = ((32 + 1) * 27 * 10000 + year - year % 1000) * 100000 * 1000000;
      break;
    }
    case DatePartType::MILLISECOND: {
      result = value - micro % 1000;
      break;
    }
    case DatePartType::MINUTE: {
      result = value - micro - min * 1000000;
      break;
    }
    case DatePartType::MONTH: {
      result = ((month * 32 + 1) * 27 * 10000 + year) * 100000 * 1000000;
      break;
    }
    case DatePartType::QUARTER: {
      result = ((((quarter - 1) * 3 + 1) * 32 + 1) * 27 * 10000 + year) *
               100000 * 1000000;
      break;
    }
    case DatePartType::SECOND: {
      result = value - micro;
      break;
    }
    case DatePartType::WEEK: {
      result =
          ((month * 32 + day - dow) * 27 * 10000 + year) * 100000 * 1000000;
      break;
    }
    case DatePartType::YEAR: {
      result = ((32 + 1) * 27 * 10000 + year) * 100000 * 1000000;
      break;
    }
    default: { result = type::PELOTON_TIMESTAMP_NULL; }
  };

  return (result);
}

}  // namespace expression
}  // namespace peloton

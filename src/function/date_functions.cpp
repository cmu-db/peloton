//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions.cpp
//
// Identification: src/function/date_functions.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/date_functions.h"

#include <date/date.h>
#include <date/iso_week.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>

#include "common/logger.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace function {

// The arguments are contained in the args vector
// (1) The first argument is the part of the date to extract
// (see DatePartType in type/types.h
// (2) The second argument is the timestamp to extract the part from
// @return The Value returned should be a type::DecimalValue that is
// constructed using type::ValueFactory
type::Value DateFunctions::Extract(const std::vector<type::Value> &args) {
  DatePartType date_part = args[0].GetAs<DatePartType>();
  uint64_t timestamp = args[1].GetAs<uint64_t>();
  type::Value result;

  if (timestamp == type::PELOTON_TIMESTAMP_NULL) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }

  uint32_t micro = timestamp % 1000000;
  timestamp /= 1000000;
  uint32_t hour_min_sec = timestamp % 100000;
  uint16_t sec = hour_min_sec % 60;
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

  uint16_t millennium = (year - 1) / 1000 + 1;
  uint16_t century = (year - 1) / 100 + 1;
  uint16_t decade = year / 10;
  uint8_t quarter = (month - 1) / 3 + 1;

  double microsecond = sec * 1000000 + micro;
  double millisecond = sec * 1000 + micro / 1000.0;
  double second = sec + micro / 1000000.0;

  date::year_month_day ymd = date::year_month_day{
      date::year{year}, date::month{month}, date::day{day}};
  iso_week::year_weeknum_weekday yww = iso_week::year_weeknum_weekday{ymd};

  date::year_month_day year_begin =
      date::year_month_day{date::year{year}, date::month{1}, date::day{1}};
  date::days duration = date::sys_days{ymd} - date::sys_days{year_begin};

  uint16_t dow = ((unsigned)yww.weekday()) == 7 ? 0 : (unsigned)yww.weekday();
  uint16_t doy = duration.count() + 1;
  uint16_t week = (unsigned)yww.weeknum();

  switch (date_part) {
    case DatePartType::CENTURY: {
      result = type::ValueFactory::GetDecimalValue(century);
      break;
    }
    case DatePartType::DAY: {
      result = type::ValueFactory::GetDecimalValue(day);
      break;
    }
    case DatePartType::DECADE: {
      result = type::ValueFactory::GetDecimalValue(decade);
      break;
    }
    case DatePartType::DOW: {
      result = type::ValueFactory::GetDecimalValue(dow);
      break;
    }
    case DatePartType::DOY: {
      result = type::ValueFactory::GetDecimalValue(doy);
      break;
    }
    case DatePartType::HOUR: {
      result = type::ValueFactory::GetDecimalValue(hour);
      break;
    }
    case DatePartType::MICROSECOND: {
      result = type::ValueFactory::GetDecimalValue(microsecond);
      break;
    }
    case DatePartType::MILLENNIUM: {
      result = type::ValueFactory::GetDecimalValue(millennium);
      break;
    }
    case DatePartType::MILLISECOND: {
      result = type::ValueFactory::GetDecimalValue(millisecond);
      break;
    }
    case DatePartType::MINUTE: {
      result = type::ValueFactory::GetDecimalValue(min);
      break;
    }
    case DatePartType::MONTH: {
      result = type::ValueFactory::GetDecimalValue(month);
      break;
    }
    case DatePartType::QUARTER: {
      result = type::ValueFactory::GetDecimalValue(quarter);
      break;
    }
    case DatePartType::SECOND: {
      result = type::ValueFactory::GetDecimalValue(second);
      break;
    }
    case DatePartType::WEEK: {
      result = type::ValueFactory::GetDecimalValue(week);
      break;
    }
    case DatePartType::YEAR: {
      result = type::ValueFactory::GetDecimalValue(year);
      break;
    }
    default: {
      result = type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
    }
  };

  return (result);
}

// This now is not what postgres does.
// Postgres is returning the time when the transaction begins
// We are here intead generating a new time when this function
// is called
int64_t DateFunctions::Now() {
  uint64_t time_stamp;
  struct timeval tv;
  struct tm *time_info;

  uint64_t hour_min_sec_base = 1000000; //us to sec
  uint64_t year_base = hour_min_sec_base * 100000;
  uint64_t day_base = year_base * 10000 * 27; // skip the time zone
  uint64_t month_base = day_base * 32;

  gettimeofday(&tv, NULL);
  time_info = gmtime(&(tv.tv_sec));

  uint32_t hour_min_sec = time_info->tm_hour * 3600 + 
                          time_info->tm_min * 60 + 
                          time_info->tm_sec;
  // EPOCH time start from 1970
  uint16_t year = time_info->tm_year + 1900;
  uint16_t day = time_info->tm_mday;
  uint16_t month = time_info->tm_mon + 1; // tm_mon is from 0 - 11

  time_stamp = tv.tv_usec;
  time_stamp += hour_min_sec_base * hour_min_sec;
  time_stamp += year_base * year;
  time_stamp += day_base * day;
  time_stamp += month_base * month;

  return time_stamp;
}

type::Value DateFunctions::_Now(const UNUSED_ATTRIBUTE std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 0);
  int64_t now = Now();
  return type::ValueFactory::GetTimestampValue(now);
}

}  // namespace expression
}  // namespace peloton

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

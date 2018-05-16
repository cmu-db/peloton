//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions.cpp
//
// Identification: src/function/date_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/date_functions.h"

#include <date/date.h>
#include <date/iso_week.h>
#include <sys/time.h>

#include "codegen/runtime_functions.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace function {

// This implementation of Now() is **not** what postgres does. Postgres is
// returning the time when the transaction begins. We are here instead
// generating a new time when this function is called.
int64_t DateFunctions::Now() {
  uint64_t time_stamp;
  struct timeval tv;
  struct tm *time_info;

  uint64_t hour_min_sec_base = 1000000;  // us to sec
  uint64_t year_base = hour_min_sec_base * 100000;
  uint64_t day_base = year_base * 10000 * 27;  // skip the time zone
  uint64_t month_base = day_base * 32;

  gettimeofday(&tv, NULL);
  time_info = gmtime(&(tv.tv_sec));

  uint32_t hour_min_sec =
      time_info->tm_hour * 3600 + time_info->tm_min * 60 + time_info->tm_sec;
  // EPOCH time start from 1970
  uint16_t year = time_info->tm_year + 1900;
  uint16_t day = time_info->tm_mday;
  uint16_t month = time_info->tm_mon + 1;  // tm_mon is from 0 - 11

  time_stamp = tv.tv_usec;
  time_stamp += hour_min_sec_base * hour_min_sec;
  time_stamp += year_base * year;
  time_stamp += day_base * day;
  time_stamp += month_base * month;

  return time_stamp;
}

type::Value DateFunctions::_Now(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &args) {
  PELOTON_ASSERT(args.empty());
  return type::ValueFactory::GetTimestampValue(Now());
}

int32_t DateFunctions::DateToJulian(int32_t year, int32_t month, int32_t day) {
  // From Postgres date2j()

  if (month > 2) {
    month += 1;
    year += 4800;
  } else {
    month += 13;
    year += 4799;
  }

  int32_t century = year / 100;

  int32_t julian = year * 365 - 32167;
  julian += year / 4 - century + century / 4;
  julian += 7834 * month / 256 + day;

  return julian;
}

void DateFunctions::JulianToDate(int32_t julian_date, int32_t &year,
                                 int32_t &month, int32_t &day) {
  // From Postgres j2date()

  uint32_t julian = static_cast<uint32_t>(julian_date);
  julian += 32044;

  uint32_t quad = julian / 146097;

  uint32_t extra = (julian - quad * 146097) * 4 + 3;
  julian += 60 + quad * 3 + extra / 146097;
  quad = julian / 1461;
  julian -= quad * 1461;

  int32_t y = julian * 4 / 1461;
  julian = ((y != 0) ? (julian + 305) % 365 : (julian + 306) % 366) + 123;
  y += quad * 4;

  // Set year
  year = static_cast<uint32_t>(y - 4800);
  quad = julian * 2141 / 65536;

  // Set day
  day = julian - 7834 * quad / 256;

  // Set month
  month = (quad + 10) % 12 + 1;
}

namespace {

template <typename T>
bool TryParseInt(const char *&data, const char *end, T &out) {
  static_assert(std::is_integral<T>::value,
                "ParseInt() must only be called with integer types");

  // Initialize
  out = 0;

  // Trim leading whitespace
  while (*data == ' ') {
    data++;
  }

  // Return if no more data
  if (data == end) {
    return false;
  }

  const char *snapshot = data;
  while (data != end) {
    if (*data < '0' || *data > '9') {
      // Not a valid integer, stop
      break;
    }

    // Update running sum
    out = (out * 10) + (*data - '0');

    // Move along
    data++;
  }

  return snapshot != data;
}

}  // namespace

int32_t DateFunctions::InputDate(
    UNUSED_ATTRIBUTE const codegen::type::Type &type, const char *data,
    uint32_t len) {
  // Okay, Postgres supports a crap-tonne of different date-time and timestamp
  // formats. I don't want to spend time implementing them all. For now, let's
  // cover the most common formats: yyyy-mm-dd

  const char *curr_ptr = data;
  const char *end = data + len;

  uint32_t nums[3] = {0, 0, 0};
  uint32_t year, month, day;

  for (uint32_t i = 0; i < 3; i++) {
    bool parsed = TryParseInt(curr_ptr, end, nums[i]);

    bool unexpected_next_char = (*curr_ptr != '-' && *curr_ptr != '/');
    if (!parsed || (i != 2 && unexpected_next_char)) {
      goto unsupported;
    }

    curr_ptr++;
  }

  // Looks okay ... let's check the components.
  year = nums[0], month = nums[1], day = nums[2];

  if (month == 0 || month > 12 || day == 0 || day > 31) {
    goto unsupported;
  }

  switch (month) {
    case 2: {
      uint32_t days_in_feb =
          ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) ? 29 : 28;
      if (day > days_in_feb) {
        goto unsupported;
      }
      break;
    }
    case 4:
    case 6:
    case 9:
    case 11: {
      if (day > 30) {
        goto unsupported;
      }
      break;
    }
    default: {
      if (day > 31) {
        goto unsupported;
      }
      break;
    }
  }

  return DateToJulian(year, month, day);

unsupported:
  codegen::RuntimeFunctions::ThrowInvalidInputStringException();
  __builtin_unreachable();
}

}  // namespace expression
}  // namespace peloton

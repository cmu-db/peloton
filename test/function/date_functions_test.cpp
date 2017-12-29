//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions_test.cpp
//
// Identification: test/expression/date_functions_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/harness.h"

#include "function/date_functions.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

namespace test {

class DateFunctionsTests : public PelotonTest {};

/**
 * Helper method for DateFunctions::Extract
 * It packages up the inputs into the right format
 * and checks whether we get the correct result.
 */
void ExtractTestHelper(DatePartType part, std::string &date,
                       type::Value expected) {
  // DateFunctions::Extract expects to get an array as its input
  // The first argument is an IntegerValue of the DatePartType
  // The second argument is a TimestampValue of the date
  std::vector<type::Value> args = {
      type::ValueFactory::GetIntegerValue(static_cast<int>(part)),
      type::ValueFactory::CastAsTimestamp(
          type::ValueFactory::GetVarcharValue(date))};

  // Invoke the Extract method and get back the result
  auto result = function::DateFunctions::Extract(args);

  // Check that result is *not* null
  EXPECT_FALSE(result.IsNull());
  // Then check that it equals our expected value
  LOG_TRACE("COMPARE: %s = %s\n", expected.ToString().c_str(),
            result.ToString().c_str());
  EXPECT_EQ(CmpBool::TRUE, expected.CompareEquals(result));
}
// Invoke DateFunctions::Now()
TEST_F(DateFunctionsTests, NowTest) {
  auto result1 = function::DateFunctions::Now();
  sleep(1);
  auto result2 = function::DateFunctions::Now();
  EXPECT_GT(result2, result1);
}
// Invoke DateFunctions::Extract(NULL)
TEST_F(DateFunctionsTests, NullExtractTest) {
  std::vector<type::Value> args = {
      type::ValueFactory::GetIntegerValue(
          static_cast<int>(DatePartType::MINUTE)),
      type::ValueFactory::GetNullValueByType(type::TypeId::TIMESTAMP)};
  auto result = function::DateFunctions::Extract(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(DateFunctionsTests, ExtractCenturyTest) {
  std::string date_template = "%04d-01-11 01:11:11.111111+11";
  for (int year = 1750; year <= 2050; year += 25) {
    std::string date = StringUtil::Format(date_template, year);
    auto expected = type::ValueFactory::GetDecimalValue(
        static_cast<int>(std::ceil(year / 100.0)));
    ExtractTestHelper(DatePartType::CENTURY, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractMillenniumTest) {
  std::string date_template = "%04d-01-11 01:11:11.111111+11";
  for (int year = 500; year <= 3000; year += 500) {
    std::string date = StringUtil::Format(date_template, year);
    auto expected = type::ValueFactory::GetDecimalValue(
        static_cast<int>(std::ceil(year / 1000.0)));
    ExtractTestHelper(DatePartType::MILLENNIUM, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractDayTest) {
  std::string date_template = "2017-05-%02d 01:11:11.111111+11";
  for (int day = 1; day <= 31; day++) {
    std::string date = StringUtil::Format(date_template, day);
    auto expected = type::ValueFactory::GetDecimalValue(day);
    ExtractTestHelper(DatePartType::DAY, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractDecadeTest) {
  std::string date_template = "%d-01-11 01:11:11.111111+11";
  for (int year = 1750; year <= 2050; year += 25) {
    std::string date = StringUtil::Format(date_template, year);
    auto expected = type::ValueFactory::GetDecimalValue(
        static_cast<int>(std::floor(year / 10.0)));
    ExtractTestHelper(DatePartType::DECADE, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractDOWTest) {
  // '2017-05-07 00:00:00.000000+00' is a Sunday
  // We assume that the DOW starts on Sundays
  int start = 7;
  std::string date_template = "2017-05-%02d 00:00:00.000000+00";
  for (int day = 0; day < 7; day++) {
    std::string date = StringUtil::Format(date_template, start + day);
    auto expected = type::ValueFactory::GetDecimalValue(day);
    ExtractTestHelper(DatePartType::DOW, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractDOYTest) {
  // January 1st is day 1
  // Let's start at Feb 1st, which is day 32
  std::string date_template = "2017-02-%02d 00:00:00.000000+00";
  int start = 31;
  for (int day = 1; day <= 28; day++) {
    std::string date = StringUtil::Format(date_template, day);
    auto expected = type::ValueFactory::GetDecimalValue(start + day);
    ExtractTestHelper(DatePartType::DOY, date, expected);
  }
}

// TEST_F(DateFunctionsTests, ExtractEpochTest) {
//  // WARNING: We might be getting screwed with timezones
//  // Postgres is giving me times that are fives hours behind the local time,
//  // so I am adjusting them here
//  // START TIME: '2017-01-19 00:00:00.11111'
//  double start_epoch = 1484802000.11111;
//  std::string date_template = "2017-01-19 %d:00:00.11111+00";
//  for (int hour = 0; hour <= 23; hour++) {
//    std::string date = StringUtil::Format(date_template, hour);
//    auto expected =
//        type::ValueFactory::GetDecimalValue(start_epoch + (hour * 60 * 60));
//    ExtractTestHelper(DatePartType::EPOCH, date, expected);
//  }
//}

TEST_F(DateFunctionsTests, ExtractHourTest) {
  std::string date_template = "2017-05-01 %02d:11:11.111111+11";
  for (int hour = 0; hour <= 23; hour++) {
    std::string date = StringUtil::Format(date_template, hour);
    auto expected = type::ValueFactory::GetDecimalValue(hour);
    ExtractTestHelper(DatePartType::HOUR, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractMicrosecondTest) {
  std::string date_template = "2017-05-01 11:11:%02d.999999+00";
  for (int second = 0; second <= 59; second++) {
    std::string date = StringUtil::Format(date_template, second);
    auto expected =
        type::ValueFactory::GetDecimalValue((second * 1000000) + 999999);
    ExtractTestHelper(DatePartType::MICROSECOND, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractMillisecondTest) {
  std::string date_template = "2017-05-01 11:11:%02d.999999+00";
  for (int second = 0; second <= 59; second++) {
    std::string date = StringUtil::Format(date_template, second);
    auto expected =
        type::ValueFactory::GetDecimalValue((second * 1000) + 999.999);
    ExtractTestHelper(DatePartType::MILLISECOND, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractMinuteTest) {
  std::string date_template = "2017-05-01 01:%02d:11.111111+11";
  for (int minute = 0; minute <= 59; minute++) {
    std::string date = StringUtil::Format(date_template, minute);
    auto expected = type::ValueFactory::GetDecimalValue(minute);
    ExtractTestHelper(DatePartType::MINUTE, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractMonthTest) {
  std::string date_template = "2017-%02d-01 01:00:11.111111+11";
  for (int month = 1; month <= 12; month++) {
    std::string date = StringUtil::Format(date_template, month);
    auto expected = type::ValueFactory::GetDecimalValue(month);
    ExtractTestHelper(DatePartType::MONTH, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractQuarterTest) {
  int quarter = 1;
  std::string date_template = "2017-%02d-01 01:00:11.111111+11";
  for (int month = 1; month <= 12; month += 3, quarter++) {
    std::string date = StringUtil::Format(date_template, month);
    auto expected = type::ValueFactory::GetDecimalValue(quarter);
    ExtractTestHelper(DatePartType::QUARTER, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractSecondTest) {
  std::string date_template = "2017-01-01 01:00:%02d.111111+00";
  for (int second = 0; second <= 59; second++) {
    std::string date = StringUtil::Format(date_template, second);
    auto expected = type::ValueFactory::GetDecimalValue(second + 0.111111);
    ExtractTestHelper(DatePartType::SECOND, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractWeekTest) {
  // Jan 2nd is week 1
  // Go until Jan 31st
  int week = 1;
  std::string date_template = "2017-01-%02d 01:11:11.111111+11";
  for (int day = 2; day <= 31; day += 7, week++) {
    std::string date = StringUtil::Format(date_template, day);
    auto expected = type::ValueFactory::GetDecimalValue(week);
    ExtractTestHelper(DatePartType::WEEK, date, expected);
  }
}

TEST_F(DateFunctionsTests, ExtractYearTest) {
  std::string date_template = "%04d-01-11 01:11:11.111111+11";
  for (int year = 1900; year <= 2050; year++) {
    std::string date = StringUtil::Format(date_template, year);
    auto expected = type::ValueFactory::GetDecimalValue(year);
    ExtractTestHelper(DatePartType::YEAR, date, expected);
  }
}

TEST_F(DateFunctionsTests, SpeedTest) {
  std::string date = "2018-08-18 03:44:55.666666+11";

  // <PART> <EXPECTED>
  // You can generate the expected value in postgres using this SQL:
  // SELECT EXTRACT(MILLISECONDS
  //                FROM CAST('2017-01-01 12:13:14.999999+00' AS TIMESTAMP));
  std::vector<std::pair<DatePartType, double>> data = {
      std::make_pair(DatePartType::CENTURY, 21),
      std::make_pair(DatePartType::DAY, 18),
      std::make_pair(DatePartType::DECADE, 201),
      std::make_pair(DatePartType::DOW, 6),
      std::make_pair(DatePartType::DOY, 230),
      // std::make_pair(DatePartType::EPOCH, 1534563895.666670),
      std::make_pair(DatePartType::HOUR, 3),
      std::make_pair(DatePartType::MICROSECOND, 55666666.000000),
      std::make_pair(DatePartType::MILLENNIUM, 3),
      std::make_pair(DatePartType::MILLISECOND, 55666.666000),
      std::make_pair(DatePartType::MINUTE, 44),
      std::make_pair(DatePartType::MONTH, 8),
      std::make_pair(DatePartType::QUARTER, 3),
      std::make_pair(DatePartType::SECOND, 55.666666),
      std::make_pair(DatePartType::WEEK, 33),
      std::make_pair(DatePartType::YEAR, 2018),
  };

  // Invoke the Extract function 200 times per DatePartType
  // This used to be 2m when we tested everyone in class,
  // but then that takes too long...
  for (int i = 0; i < 200; i++) {
    for (auto x : data) {
      type::Value expected = type::ValueFactory::GetDecimalValue(x.second);
      ExtractTestHelper(x.first, date, expected);
    }
  }
}

TEST_F(DateFunctionsTests, ExtractTest) {
  std::string date = "2017-01-01 12:13:14.999999+00";

  // <PART> <EXPECTED>
  // You can generate the expected value in postgres using this SQL:
  // SELECT EXTRACT(MILLISECONDS
  //                FROM TIMESTAMP '2017-01-01 12:13:14.999999+00');
  std::vector<std::pair<DatePartType, double>> data = {
      std::make_pair(DatePartType::CENTURY, 21),
      std::make_pair(DatePartType::DECADE, 201),
      std::make_pair(DatePartType::DOW, 0),
      std::make_pair(DatePartType::DOY, 1),
      std::make_pair(DatePartType::YEAR, 2017),
      std::make_pair(DatePartType::MONTH, 1),
      std::make_pair(DatePartType::DAY, 1),
      std::make_pair(DatePartType::HOUR, 12),
      std::make_pair(DatePartType::MINUTE, 13),

      // Note that we support these DatePartTypes with and without
      // a trailing 's' at the end.
      std::make_pair(DatePartType::SECOND, 14.999999),
      std::make_pair(DatePartType::SECONDS, 14.999999),
      std::make_pair(DatePartType::MILLISECOND, 14999.999),
      std::make_pair(DatePartType::MILLISECONDS, 14999.999),
  };

  for (auto x : data) {
    type::Value expected = type::ValueFactory::GetDecimalValue(x.second);
    ExtractTestHelper(x.first, date, expected);
  }
}

}  // namespace test
}  // namespace peloton

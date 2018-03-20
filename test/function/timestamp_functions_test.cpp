//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions_test.cpp
//
// Identification: test/function/timestamp_functions_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/harness.h"

#include "function/timestamp_functions.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

namespace test {

class TimestampFunctionsTests : public PelotonTest {};

/**
 * Helper method for TimestampFunctions::DateTrunc
 * It packages up the inputs into the right format
 * and checks whether we get the correct result.
 */
void DateTruncTestHelper(DatePartType part, std::string &date,
                         std::string &expected) {
  // The first argument is an VarcharValue of the DatePartType
  // The second argument is a TimestampValue of the date
  std::string string_date_part = DatePartTypeToString(part);
  const char *char_date_part = string_date_part.c_str();
  uint64_t int_date = type::ValueFactory::CastAsTimestamp(
                          type::ValueFactory::GetVarcharValue(date))
                          .GetAs<uint64_t>();

  // The expected return value
  uint64_t int_expected = type::ValueFactory::CastAsTimestamp(
                              type::ValueFactory::GetVarcharValue(expected))
                              .GetAs<uint64_t>();

  // Invoke the DateTrunc method and get back the result
  auto result =
      function::TimestampFunctions::DateTrunc(char_date_part, int_date);

  // <PART> <EXPECTED>
  // You can generate the expected value in postgres using this SQL:
  // SELECT date_trunc('day', TIMESTAMP '2016-12-07 13:26:02.123456-05') at
  // time zone 'est';

  // Check that result is *not* null
  EXPECT_NE(type::PELOTON_TIMESTAMP_NULL, result);
  // Then check that it equals our expected value
  LOG_TRACE("COMPARE: %s = %" PRId64 "\n", expected.c_str(),
            static_cast<long>(result));
  EXPECT_EQ(int_expected, result);
}

// Invoke TimestampFunctions::DateTrunc(NULL)
TEST_F(TimestampFunctionsTests, NullDateTruncTest) {
  auto result = function::TimestampFunctions::DateTrunc(
      "hour", type::PELOTON_TIMESTAMP_NULL);
  EXPECT_EQ(type::PELOTON_TIMESTAMP_NULL, result);
}

TEST_F(TimestampFunctionsTests, DateTruncCenturyTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2001-01-01 00:00:00-05";

  DateTruncTestHelper(DatePartType::CENTURY, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncMillenniumTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2001-01-01 00:00:00.000000-05";

  DateTruncTestHelper(DatePartType::MILLENNIUM, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncDayTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-07 00:00:00-05";

  DateTruncTestHelper(DatePartType::DAY, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncDecadeTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2010-01-01 00:00:00-05";

  DateTruncTestHelper(DatePartType::DECADE, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncHourTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-07 13:00:00-05";

  DateTruncTestHelper(DatePartType::HOUR, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncMicrosecondTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-07 13:26:02.123456-05";

  DateTruncTestHelper(DatePartType::MICROSECOND, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncMillisecondTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-07 13:26:02.123000-05";

  DateTruncTestHelper(DatePartType::MILLISECOND, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncMinuteTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-07 13:26:00-05";

  DateTruncTestHelper(DatePartType::MINUTE, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncMonthTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-01 00:00:00-05";

  DateTruncTestHelper(DatePartType::MONTH, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncQuarterTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-10-01 00:00:00-05";

  DateTruncTestHelper(DatePartType::QUARTER, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncSecondTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-07 13:26:02-05";

  DateTruncTestHelper(DatePartType::SECOND, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncWeekTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-12-05 00:00:00-05";

  DateTruncTestHelper(DatePartType::WEEK, date, expected);
}

TEST_F(TimestampFunctionsTests, DateTruncYearTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016-01-01 00:00:00-05";

  DateTruncTestHelper(DatePartType::YEAR, date, expected);
}

/**
 * Helper method for TimestampFunctions::DatePart
 * It packages up the inputs into the right format
 * and checks whether we get the correct result.
 */
void DatePartTestHelper(DatePartType part, std::string &date,
                        std::string &expected) {
  // The first argument is an VarcharValue of the DatePartType
  // The second argument is a TimestampValue of the date
  std::string string_date_part = DatePartTypeToString(part);
  const char *char_date_part = string_date_part.c_str();
  uint64_t int_date = type::ValueFactory::CastAsTimestamp(
                          type::ValueFactory::GetVarcharValue(date))
                          .GetAs<uint64_t>();

  // The expected return value
  double double_expected = type::ValueFactory::CastAsDecimal(
                               type::ValueFactory::GetVarcharValue(expected))
                               .GetAs<double>();

  // Invoke the DatePart method and get back the result
  auto result =
      function::TimestampFunctions::DatePart(char_date_part, int_date);

  LOG_INFO("COMPARED: %s, %f = %f \n", expected.c_str(), double_expected,
           result);

  // Check that result is *not* null
  EXPECT_NE(type::PELOTON_DECIMAL_NULL, result);

  // Then check that it equals our expected value
  LOG_TRACE("COMPARE: %s = %f \n", expected.c_str(), result);
  EXPECT_DOUBLE_EQ(double_expected, result);
}

// Invoke TimestampFunctions::DateTrunc(NULL)
TEST_F(TimestampFunctionsTests, NullDatePartTest) {
  auto result = function::TimestampFunctions::DatePart(
      "hour", type::PELOTON_TIMESTAMP_NULL);
  EXPECT_EQ(type::PELOTON_DECIMAL_NULL, result);
}

TEST_F(TimestampFunctionsTests, DatePartCenturyTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "21";

  DatePartTestHelper(DatePartType::CENTURY, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartMillenniumTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "3";

  DatePartTestHelper(DatePartType::MILLENNIUM, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartDayTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "7";

  DatePartTestHelper(DatePartType::DAY, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartDecadeTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "201";

  DatePartTestHelper(DatePartType::DECADE, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartHourTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "13";

  DatePartTestHelper(DatePartType::HOUR, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartMicrosecondTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2.123456e+06";

  DatePartTestHelper(DatePartType::MICROSECOND, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartMillisecondTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2123.456";

  DatePartTestHelper(DatePartType::MILLISECOND, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartMinuteTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "26";

  DatePartTestHelper(DatePartType::MINUTE, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartMonthTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "12";

  DatePartTestHelper(DatePartType::MONTH, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartQuarterTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "4";

  DatePartTestHelper(DatePartType::QUARTER, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartSecondTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2.123456";

  DatePartTestHelper(DatePartType::SECOND, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartWeekTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "49";

  DatePartTestHelper(DatePartType::WEEK, date, expected);
}

TEST_F(TimestampFunctionsTests, DatePartYearTest) {
  std::string date = "2016-12-07 13:26:02.123456-05";
  std::string expected = "2016";

  DatePartTestHelper(DatePartType::YEAR, date, expected);
}

}  // namespace test
}  // namespace peloton

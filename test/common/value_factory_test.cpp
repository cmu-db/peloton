//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_factory_test.cpp
//
// Identification: test/common/value_factory_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// this is ugly but tests shouldn't be friend classes
#define VALUE_TESTS

#include <limits.h>
#include <iostream>
#include <cstdint>
#include <cmath>
#include <memory>

#include "common/serializeio.h"
#include "common/value_factory.h"
#include "common/value_peeker.h"
#include "common/harness.h"


namespace peloton {
namespace test {

class ValueFactoryTests : public PelotonTest {};

#define RANDOM_DECIMAL() ((double)rand() / (double)rand())

int8_t RANDOM8() {
  return ((rand() % (SCHAR_MAX * 2 - 1)) - (SCHAR_MAX - 1));
}

int16_t RANDOM16() {
  return ((rand() % (SHRT_MAX * 2 - 1)) - (SHRT_MAX - 1));
}

int32_t RANDOM32() {
  int32_t ret = (((size_t)(rand()) << 1) | ((size_t)(rand()) & 0x1));
  if (ret != common::PELOTON_INT32_NULL)
    return ret;
  return 1;
}

int64_t RANDOM64() {
  int64_t ret = (((size_t)(rand()) << 33) | ((size_t)(rand()) << 2) |
                 ((size_t)(rand()) & 0x3));
  if (ret != common::PELOTON_INT64_NULL)
    return ret;
  return 1;
}

TEST_F(ValueFactoryTests, PeekValueTest) {
  common::Value v1(common::Type::TINYINT, (int8_t)common::PELOTON_INT8_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekTinyInt(v1), common::PELOTON_INT8_MAX);
  common::Value v2(common::Type::SMALLINT, (int16_t)common::PELOTON_INT16_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekSmallInt(v2), common::PELOTON_INT16_MAX);
  common::Value v3(common::Type::INTEGER, (int32_t)common::PELOTON_INT32_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekInteger(v3), common::PELOTON_INT32_MAX);
  common::Value v4(common::Type::BIGINT, (int64_t)common::PELOTON_INT64_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekBigInt(v4), common::PELOTON_INT64_MAX);
  common::Value v5(common::Type::DECIMAL, (double)common::PELOTON_DECIMAL_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekDouble(v5), common::PELOTON_DECIMAL_MAX);
  common::Value v6(common::Type::BOOLEAN, true);
  EXPECT_EQ(common::ValuePeeker::PeekBoolean(v6), true);
  std::string str = "hello";
  common::Value v7(common::Type::VARCHAR, str);
  EXPECT_EQ(v7.GetData(), str);
}

TEST_F(ValueFactoryTests, CastTest) {
  common::Value v1(common::ValueFactory::CastAsBigInt(
    common::Value(common::Type::INTEGER, (int32_t)common::PELOTON_INT32_MAX)));
  EXPECT_EQ(v1.GetTypeId(), common::Type::BIGINT);
  EXPECT_EQ(v1.GetAs<int64_t>(), common::PELOTON_INT32_MAX);

  common::Value v2(common::ValueFactory::CastAsBigInt(
    common::Value(common::Type::SMALLINT, (int16_t)common::PELOTON_INT16_MAX)));
  EXPECT_EQ(v2.GetTypeId(), common::Type::BIGINT);

  EXPECT_THROW(common::ValueFactory::CastAsBigInt(common::Value(common::Type::BOOLEAN, 0)), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsSmallInt(common::Value(common::Type::INTEGER, common::PELOTON_INT32_MAX)),
                                            peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsTinyInt(common::Value(common::Type::INTEGER,common::PELOTON_INT32_MAX)),
                                           peloton::Exception);

  common::Value v3(common::ValueFactory::CastAsVarchar(common::ValueFactory::GetVarcharValue("hello")));
  EXPECT_EQ(v3.GetTypeId(), common::Type::VARCHAR);

  common::Value v4(common::ValueFactory::Clone(v3));
  common::Value cmp3(v3.CompareEquals(v4));
  EXPECT_TRUE(cmp3.IsTrue());

  common::Value v5(common::ValueFactory::CastAsVarchar(common::Value(common::Type::TINYINT, (int8_t)common::PELOTON_INT8_MAX)));
  EXPECT_EQ(v5.ToString(), "127");
  common::Value v6(common::ValueFactory::CastAsVarchar(common::Value(common::Type::BIGINT, (int64_t)common::PELOTON_INT64_MAX)));
  EXPECT_EQ(v6.ToString(), "9223372036854775807");

  std::string str1("9999-12-31 23:59:59.999999+14");
  common::Value v7(common::ValueFactory::CastAsTimestamp(common::Value(common::Type::VARCHAR, str1)));
  EXPECT_EQ(v7.ToString(), str1);
  std::string str2("9999-12-31 23:59:59-01");
  common::Value v77(common::ValueFactory::CastAsTimestamp(common::Value(common::Type::VARCHAR, str2)));
  EXPECT_EQ(v77.ToString(), "9999-12-31 23:59:59.000000-01");
  EXPECT_THROW(common::ValueFactory::CastAsTimestamp(common::Value(common::Type::VARCHAR, "1900-02-29 23:59:59.999999+12")),
                                             peloton::Exception);

  common::Value v8(common::ValueFactory::CastAsBigInt(common::Value(common::Type::VARCHAR, "9223372036854775807")));
  EXPECT_EQ(v8.GetAs<int64_t>(), 9223372036854775807);
  EXPECT_THROW(common::ValueFactory::CastAsBigInt(common::Value(common::Type::VARCHAR, "9223372036854775808")), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsBigInt(common::Value(common::Type::VARCHAR, "-9223372036854775808")), peloton::Exception);

  common::Value v9(common::ValueFactory::CastAsInteger(common::Value(common::Type::VARCHAR, "2147483647")));
  EXPECT_EQ(v9.GetAs<int32_t>(), 2147483647);
  EXPECT_THROW(common::ValueFactory::CastAsInteger(common::Value(common::Type::VARCHAR, "-2147483648")), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsInteger(common::Value(common::Type::VARCHAR, "2147483648")), peloton::Exception);

  common::Value v10(common::ValueFactory::CastAsSmallInt(common::Value(common::Type::VARCHAR, "32767")));
  EXPECT_EQ(v10.GetAs<int16_t>(), 32767);
  EXPECT_THROW(common::ValueFactory::CastAsSmallInt(common::Value(common::Type::VARCHAR, "-32768")), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsSmallInt(common::Value(common::Type::VARCHAR, "32768")), peloton::Exception);

  common::Value v11(common::ValueFactory::CastAsTinyInt(common::Value(common::Type::VARCHAR, "127")));
  EXPECT_EQ(v11.GetAs<int8_t>(), 127);
  EXPECT_THROW(common::ValueFactory::CastAsTinyInt(common::Value(common::Type::VARCHAR, "-128")), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsTinyInt(common::Value(common::Type::VARCHAR, "128")), peloton::Exception);
}

TEST_F(ValueFactoryTests, SerializationTest) {
  peloton::CopySerializeOutput out;
  common::Value(common::Type::TINYINT, (int8_t)common::PELOTON_INT8_MAX).SerializeTo(out);
  common::Value(common::Type::TINYINT, (int8_t)common::PELOTON_INT8_MIN).SerializeTo(out);
  common::Value(common::Type::SMALLINT, (int16_t)common::PELOTON_INT16_MAX).SerializeTo(out);
  common::Value(common::Type::SMALLINT, (int16_t)common::PELOTON_INT16_MIN).SerializeTo(out);
  common::Value(common::Type::INTEGER, (int32_t)common::PELOTON_INT32_MAX).SerializeTo(out);
  common::Value(common::Type::INTEGER, (int32_t)common::PELOTON_INT32_MIN).SerializeTo(out);
  common::Value(common::Type::BIGINT, (int64_t)common::PELOTON_INT64_MAX).SerializeTo(out);
  common::Value(common::Type::BIGINT, (int64_t)common::PELOTON_INT64_MIN).SerializeTo(out);
  common::Value(common::Type::DECIMAL, common::PELOTON_DECIMAL_MAX).SerializeTo(out);
  common::Value(common::Type::DECIMAL, common::PELOTON_DECIMAL_MIN).SerializeTo(out);

  peloton::CopySerializeInput in(out.Data(), out.Size());
  common::Value v1 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::TINYINT)->GetTypeId(), nullptr));
  EXPECT_EQ(v1.GetTypeId(), common::Type::TINYINT);
  EXPECT_EQ(v1.GetAs<int8_t>(), common::PELOTON_INT8_MAX);
  common::Value v2 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::TINYINT)->GetTypeId(), nullptr));
  EXPECT_EQ(v2.GetTypeId(), common::Type::TINYINT);
  EXPECT_EQ(v2.GetAs<int8_t>(), common::PELOTON_INT8_MIN);
  common::Value v3 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::SMALLINT)->GetTypeId(), nullptr));
  EXPECT_EQ(v3.GetTypeId(), common::Type::SMALLINT);
  EXPECT_EQ(v3.GetAs<int16_t>(), common::PELOTON_INT16_MAX);
  common::Value v4 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::SMALLINT)->GetTypeId(), nullptr));
  EXPECT_EQ(v4.GetTypeId(), common::Type::SMALLINT);
  EXPECT_EQ(v4.GetAs<int16_t>(), common::PELOTON_INT16_MIN);
  common::Value v5 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::INTEGER)->GetTypeId(), nullptr));
  EXPECT_EQ(v5.GetTypeId(), common::Type::INTEGER);
  EXPECT_EQ(v5.GetAs<int32_t>(), common::PELOTON_INT32_MAX);
  common::Value v6 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::INTEGER)->GetTypeId(), nullptr));
  EXPECT_EQ(v6.GetTypeId(), common::Type::INTEGER);
  EXPECT_EQ(v6.GetAs<int32_t>(), common::PELOTON_INT32_MIN);
  common::Value v7 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::BIGINT)->GetTypeId(), nullptr));
  EXPECT_EQ(v7.GetTypeId(), common::Type::BIGINT);
  EXPECT_EQ(v7.GetAs<int64_t>(), common::PELOTON_INT64_MAX);
  common::Value v8 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::BIGINT)->GetTypeId(), nullptr));
  EXPECT_EQ(v8.GetTypeId(), common::Type::BIGINT);
  EXPECT_EQ(v8.GetAs<int64_t>(), common::PELOTON_INT64_MIN);

  common::Value v9 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::DECIMAL)->GetTypeId(), nullptr));
  EXPECT_EQ(v9.GetTypeId(), common::Type::DECIMAL);
  EXPECT_EQ(v9.GetAs<double>(), common::PELOTON_DECIMAL_MAX);
  common::Value v10 = (common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::DECIMAL)->GetTypeId(), nullptr));
  EXPECT_EQ(v10.GetTypeId(), common::Type::DECIMAL);
  EXPECT_EQ(v10.GetAs<double>(), common::PELOTON_DECIMAL_MIN);
}

}
}

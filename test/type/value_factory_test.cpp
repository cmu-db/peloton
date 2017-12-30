//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_factory_test.cpp
//
// Identification: test/type/value_factory_test.cpp
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

#include "type/serializeio.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"
#include "common/harness.h"


namespace peloton {
namespace test {

class ValueFactoryTests : public PelotonTest {};

const std::vector<type::TypeId> valuefactory_test_types = {
    type::TypeId::BOOLEAN,    type::TypeId::TINYINT,
    type::TypeId::SMALLINT,   type::TypeId::INTEGER,
    type::TypeId::BIGINT,     type::TypeId::DECIMAL,
    type::TypeId::TIMESTAMP,  type::TypeId::DATE
};

#define RANDOM_DECIMAL() ((double)rand() / (double)rand())

int8_t RANDOM8() {
  return ((rand() % (SCHAR_MAX * 2 - 1)) - (SCHAR_MAX - 1));
}

int16_t RANDOM16() {
  return ((rand() % (SHRT_MAX * 2 - 1)) - (SHRT_MAX - 1));
}

int32_t RANDOM32() {
  int32_t ret = (((size_t)(rand()) << 1) | ((size_t)(rand()) & 0x1));
  if (ret != type::PELOTON_INT32_NULL)
    return ret;
  return 1;
}

int64_t RANDOM64() {
  int64_t ret = (((size_t)(rand()) << 33) | ((size_t)(rand()) << 2) |
                 ((size_t)(rand()) & 0x3));
  if (ret != type::PELOTON_INT64_NULL)
    return ret;
  return 1;
}

TEST_F(ValueFactoryTests, ZeroValueTest) {
  for (auto col_type : valuefactory_test_types) {
    auto zero_val = type::ValueFactory::GetZeroValueByType(col_type);
    EXPECT_FALSE(zero_val.IsNull());
  }
}

TEST_F(ValueFactoryTests, PeekValueTest) {
  type::Value v1(type::TypeId::TINYINT, (int8_t)type::PELOTON_INT8_MAX);
  EXPECT_EQ(type::ValuePeeker::PeekTinyInt(v1), type::PELOTON_INT8_MAX);
  type::Value v2(type::TypeId::SMALLINT, (int16_t)type::PELOTON_INT16_MAX);
  EXPECT_EQ(type::ValuePeeker::PeekSmallInt(v2), type::PELOTON_INT16_MAX);
  type::Value v3(type::TypeId::INTEGER, (int32_t)type::PELOTON_INT32_MAX);
  EXPECT_EQ(type::ValuePeeker::PeekInteger(v3), type::PELOTON_INT32_MAX);
  type::Value v4(type::TypeId::BIGINT, (int64_t)type::PELOTON_INT64_MAX);
  EXPECT_EQ(type::ValuePeeker::PeekBigInt(v4), type::PELOTON_INT64_MAX);
  type::Value v5(type::TypeId::DECIMAL, (double)type::PELOTON_DECIMAL_MAX);
  EXPECT_EQ(type::ValuePeeker::PeekDouble(v5), type::PELOTON_DECIMAL_MAX);
  type::Value v6(type::TypeId::BOOLEAN, true);
  EXPECT_EQ(type::ValuePeeker::PeekBoolean(v6), true);
  std::string str = "hello";
  type::Value v7(type::TypeId::VARCHAR, str);
  EXPECT_EQ(v7.GetData(), str);
  type::Value v8 = type::ValueFactory::GetVarcharValue("hello");
  EXPECT_EQ(v8.ToString(), str);
}

TEST_F(ValueFactoryTests, CastTest) {
  type::Value v1(type::ValueFactory::CastAsBigInt(
    type::Value(type::TypeId::INTEGER, (int32_t)type::PELOTON_INT32_MAX)));
  EXPECT_EQ(v1.GetTypeId(), type::TypeId::BIGINT);
  EXPECT_EQ(v1.GetAs<int64_t>(), type::PELOTON_INT32_MAX);

  type::Value v2(type::ValueFactory::CastAsBigInt(
    type::Value(type::TypeId::SMALLINT, (int16_t)type::PELOTON_INT16_MAX)));
  EXPECT_EQ(v2.GetTypeId(), type::TypeId::BIGINT);

  EXPECT_THROW(type::ValueFactory::CastAsBigInt(type::Value(type::TypeId::BOOLEAN, 0)), peloton::Exception);
  EXPECT_THROW(type::ValueFactory::CastAsSmallInt(type::Value(type::TypeId::INTEGER, type::PELOTON_INT32_MAX)),
                                            peloton::Exception);
  EXPECT_THROW(type::ValueFactory::CastAsTinyInt(type::Value(type::TypeId::INTEGER,type::PELOTON_INT32_MAX)),
                                           peloton::Exception);

  type::Value v3(type::ValueFactory::CastAsVarchar(type::ValueFactory::GetVarcharValue("hello")));
  EXPECT_EQ(v3.GetTypeId(), type::TypeId::VARCHAR);

  type::Value v4(type::ValueFactory::Clone(v3));
  EXPECT_TRUE(v3.CompareEquals(v4) == CmpBool::TRUE);

  type::Value v5(type::ValueFactory::CastAsVarchar(type::Value(type::TypeId::TINYINT, (int8_t)type::PELOTON_INT8_MAX)));
  EXPECT_EQ(v5.ToString(), "127");
  type::Value v6(type::ValueFactory::CastAsVarchar(type::Value(type::TypeId::BIGINT, (int64_t)type::PELOTON_INT64_MAX)));
  EXPECT_EQ(v6.ToString(), "9223372036854775807");

  std::string str1("9999-12-31 23:59:59.999999+14");
  type::Value v7(type::ValueFactory::CastAsTimestamp(type::Value(type::TypeId::VARCHAR, str1)));
  EXPECT_EQ(v7.ToString(), str1);
  std::string str2("9999-12-31 23:59:59-01");
  type::Value v77(type::ValueFactory::CastAsTimestamp(type::Value(type::TypeId::VARCHAR, str2)));
  EXPECT_EQ(v77.ToString(), "9999-12-31 23:59:59.000000-01");
  EXPECT_THROW(type::ValueFactory::CastAsTimestamp(type::Value(type::TypeId::VARCHAR, "1900-02-29 23:59:59.999999+12")),
                                             peloton::Exception);

  type::Value v8(type::ValueFactory::CastAsBigInt(type::Value(type::TypeId::VARCHAR, "9223372036854775807")));
  EXPECT_EQ(v8.GetAs<int64_t>(), 9223372036854775807);
  EXPECT_THROW(type::ValueFactory::CastAsBigInt(type::Value(type::TypeId::VARCHAR, "9223372036854775808")), peloton::Exception);
  EXPECT_THROW(type::ValueFactory::CastAsBigInt(type::Value(type::TypeId::VARCHAR, "-9223372036854775808")), peloton::Exception);

  type::Value v9(type::ValueFactory::CastAsInteger(type::Value(type::TypeId::VARCHAR, "2147483647")));
  EXPECT_EQ(v9.GetAs<int32_t>(), 2147483647);
  EXPECT_THROW(type::ValueFactory::CastAsInteger(type::Value(type::TypeId::VARCHAR, "-2147483648")), peloton::Exception);
  EXPECT_THROW(type::ValueFactory::CastAsInteger(type::Value(type::TypeId::VARCHAR, "2147483648")), peloton::Exception);

  type::Value v10(type::ValueFactory::CastAsSmallInt(type::Value(type::TypeId::VARCHAR, "32767")));
  EXPECT_EQ(v10.GetAs<int16_t>(), 32767);
  EXPECT_THROW(type::ValueFactory::CastAsSmallInt(type::Value(type::TypeId::VARCHAR, "-32768")), peloton::Exception);
  EXPECT_THROW(type::ValueFactory::CastAsSmallInt(type::Value(type::TypeId::VARCHAR, "32768")), peloton::Exception);

  type::Value v11(type::ValueFactory::CastAsTinyInt(type::Value(type::TypeId::VARCHAR, "127")));
  EXPECT_EQ(v11.GetAs<int8_t>(), 127);
  EXPECT_THROW(type::ValueFactory::CastAsTinyInt(type::Value(type::TypeId::VARCHAR, "-128")), peloton::Exception);
  EXPECT_THROW(type::ValueFactory::CastAsTinyInt(type::Value(type::TypeId::VARCHAR, "128")), peloton::Exception);
}

TEST_F(ValueFactoryTests, SerializationTest) {
  peloton::CopySerializeOutput out;
  for (auto col_type : valuefactory_test_types) {
    type::Type::GetMinValue(col_type).SerializeTo(out);
    type::Type::GetMaxValue(col_type).SerializeTo(out);
  }

  peloton::CopySerializeInput in(out.Data(), out.Size());
  for (auto col_type : valuefactory_test_types) {
    for (auto expect_min : {true, false}) {
      type::Value v = (type::Value::DeserializeFrom(in, type::Type::GetInstance(col_type)->GetTypeId(), nullptr));
      EXPECT_EQ(v.GetTypeId(), col_type);
      type::Value expected = (expect_min ?
                                type::Type::GetMinValue(col_type) :
                                type::Type::GetMaxValue(col_type));
      EXPECT_EQ(CmpBool::TRUE, v.CompareEquals(expected));
    }
  }
}

}
}

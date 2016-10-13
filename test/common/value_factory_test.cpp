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
  common::IntegerValue v1((int8_t)common::PELOTON_INT8_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekTinyInt(&v1), common::PELOTON_INT8_MAX);
  common::IntegerValue v2((int16_t)common::PELOTON_INT16_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekSmallInt(&v2), common::PELOTON_INT16_MAX);
  common::IntegerValue v3((int32_t)common::PELOTON_INT32_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekInteger(&v3), common::PELOTON_INT32_MAX);
  common::IntegerValue v4((int64_t)common::PELOTON_INT64_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekBigInt(&v4), common::PELOTON_INT64_MAX);
  common::DecimalValue v5((double)common::PELOTON_DECIMAL_MAX);
  EXPECT_EQ(common::ValuePeeker::PeekDouble(&v5), common::PELOTON_DECIMAL_MAX);
  common::BooleanValue v6(true);
  EXPECT_EQ(common::ValuePeeker::PeekBoolean(&v6), true);
  std::string str = "hello";
  common::VarlenValue v7(str, false);
  EXPECT_EQ(v7.GetData(), str);
}

TEST_F(ValueFactoryTests, CastTest) {
  std::unique_ptr<common::Value> v1(common::ValueFactory::CastAsBigInt(
    common::IntegerValue((int32_t)common::PELOTON_INT32_MAX)));
  EXPECT_EQ(v1->GetTypeId(), common::Type::BIGINT);
  EXPECT_EQ(v1->GetAs<int64_t>(), common::PELOTON_INT32_MAX);

  std::unique_ptr<common::Value> v2(common::ValueFactory::CastAsBigInt(
    common::IntegerValue((int16_t)common::PELOTON_INT16_MAX)));
  EXPECT_EQ(v2->GetTypeId(), common::Type::BIGINT);

  EXPECT_THROW(common::ValueFactory::CastAsBigInt(common::BooleanValue(0)), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsSmallInt(common::IntegerValue(common::PELOTON_INT32_MAX)),
                                            peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsTinyInt(common::IntegerValue(common::PELOTON_INT32_MAX)),
                                           peloton::Exception);

  std::unique_ptr<common::Value> v3(common::ValueFactory::CastAsVarchar(common::ValueFactory::GetVarcharValue("hello")));
  EXPECT_EQ(v3->GetTypeId(), common::Type::VARCHAR);

  std::unique_ptr<common::Value> v4(common::ValueFactory::Clone(*v3));
  std::unique_ptr<common::BooleanValue> cmp3((common::BooleanValue *)v3->CompareEquals(*v4));
  EXPECT_TRUE(cmp3->IsTrue());

  std::unique_ptr<common::Value> v5(common::ValueFactory::CastAsVarchar(common::IntegerValue((int8_t)common::PELOTON_INT8_MAX)));
  EXPECT_EQ(v5->ToString(), "127");
  std::unique_ptr<common::Value> v6(common::ValueFactory::CastAsVarchar(common::IntegerValue((int64_t)common::PELOTON_INT64_MAX)));
  EXPECT_EQ(v6->ToString(), "9223372036854775807");

  std::string str1("9999-12-31 23:59:59.999999+14");
  std::unique_ptr<common::Value> v7(common::ValueFactory::CastAsTimestamp(common::VarlenValue(str1, false)));
  EXPECT_EQ(v7->ToString(), str1);
  std::string str2("9999-12-31 23:59:59-01");
  std::unique_ptr<common::Value> v77(common::ValueFactory::CastAsTimestamp(common::VarlenValue(str2, false)));
  EXPECT_EQ(v77->ToString(), "9999-12-31 23:59:59.000000-01");
  EXPECT_THROW(common::ValueFactory::CastAsTimestamp(common::VarlenValue("1900-02-29 23:59:59.999999+12", false)),
                                             peloton::Exception);

  std::unique_ptr<common::Value> v8(common::ValueFactory::CastAsBigInt(common::VarlenValue("9223372036854775807", false)));
  EXPECT_EQ(v8->GetAs<int64_t>(), 9223372036854775807);
  EXPECT_THROW(common::ValueFactory::CastAsBigInt(common::VarlenValue("9223372036854775808", false)), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsBigInt(common::VarlenValue("-9223372036854775808", false)), peloton::Exception);

  std::unique_ptr<common::Value> v9(common::ValueFactory::CastAsInteger(common::VarlenValue("2147483647", false)));
  EXPECT_EQ(v9->GetAs<int32_t>(), 2147483647);
  EXPECT_THROW(common::ValueFactory::CastAsInteger(common::VarlenValue("-2147483648", false)), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsInteger(common::VarlenValue("2147483648", false)), peloton::Exception);

  std::unique_ptr<common::Value> v10(common::ValueFactory::CastAsSmallInt(common::VarlenValue("32767", false)));
  EXPECT_EQ(v10->GetAs<int16_t>(), 32767);
  EXPECT_THROW(common::ValueFactory::CastAsSmallInt(common::VarlenValue("-32768", false)), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsSmallInt(common::VarlenValue("32768", false)), peloton::Exception);

  std::unique_ptr<common::Value> v11(common::ValueFactory::CastAsTinyInt(common::VarlenValue("127", false)));
  EXPECT_EQ(v11->GetAs<int8_t>(), 127);
  EXPECT_THROW(common::ValueFactory::CastAsTinyInt(common::VarlenValue("-128", false)), peloton::Exception);
  EXPECT_THROW(common::ValueFactory::CastAsTinyInt(common::VarlenValue("128", false)), peloton::Exception);
}

TEST_F(ValueFactoryTests, SerializationTest) {
  peloton::CopySerializeOutput out;
  common::IntegerValue((int8_t)common::PELOTON_INT8_MAX).SerializeTo(out);
  common::IntegerValue((int8_t)common::PELOTON_INT8_MIN).SerializeTo(out);
  common::IntegerValue((int16_t)common::PELOTON_INT16_MAX).SerializeTo(out);
  common::IntegerValue((int16_t)common::PELOTON_INT16_MIN).SerializeTo(out);
  common::IntegerValue((int32_t)common::PELOTON_INT32_MAX).SerializeTo(out);
  common::IntegerValue((int32_t)common::PELOTON_INT32_MIN).SerializeTo(out);
  common::IntegerValue((int64_t)common::PELOTON_INT64_MAX).SerializeTo(out);
  common::IntegerValue((int64_t)common::PELOTON_INT64_MIN).SerializeTo(out);
  common::DecimalValue(common::PELOTON_DECIMAL_MAX).SerializeTo(out);
  common::DecimalValue(common::PELOTON_DECIMAL_MIN).SerializeTo(out);

  peloton::CopySerializeInput in(out.Data(), out.Size());
  std::unique_ptr<common::Value> v1(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::TINYINT).GetTypeId(), nullptr));
  EXPECT_EQ(v1->GetTypeId(), common::Type::TINYINT);
  EXPECT_EQ(v1->GetAs<int8_t>(), common::PELOTON_INT8_MAX);
  std::unique_ptr<common::Value> v2(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::TINYINT).GetTypeId(), nullptr));
  EXPECT_EQ(v2->GetTypeId(), common::Type::TINYINT);
  EXPECT_EQ(v2->GetAs<int8_t>(), common::PELOTON_INT8_MIN);
  std::unique_ptr<common::Value> v3(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::SMALLINT).GetTypeId(), nullptr));
  EXPECT_EQ(v3->GetTypeId(), common::Type::SMALLINT);
  EXPECT_EQ(v3->GetAs<int16_t>(), common::PELOTON_INT16_MAX);
  std::unique_ptr<common::Value> v4(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::SMALLINT).GetTypeId(), nullptr));
  EXPECT_EQ(v4->GetTypeId(), common::Type::SMALLINT);
  EXPECT_EQ(v4->GetAs<int16_t>(), common::PELOTON_INT16_MIN);
  std::unique_ptr<common::Value> v5(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::INTEGER).GetTypeId(), nullptr));
  EXPECT_EQ(v5->GetTypeId(), common::Type::INTEGER);
  EXPECT_EQ(v5->GetAs<int32_t>(), common::PELOTON_INT32_MAX);
  std::unique_ptr<common::Value> v6(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::INTEGER).GetTypeId(), nullptr));
  EXPECT_EQ(v6->GetTypeId(), common::Type::INTEGER);
  EXPECT_EQ(v6->GetAs<int32_t>(), common::PELOTON_INT32_MIN);
  std::unique_ptr<common::Value> v7(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::BIGINT).GetTypeId(), nullptr));
  EXPECT_EQ(v7->GetTypeId(), common::Type::BIGINT);
  EXPECT_EQ(v7->GetAs<int64_t>(), common::PELOTON_INT64_MAX);
  std::unique_ptr<common::Value> v8(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::BIGINT).GetTypeId(), nullptr));
  EXPECT_EQ(v8->GetTypeId(), common::Type::BIGINT);
  EXPECT_EQ(v8->GetAs<int64_t>(), common::PELOTON_INT64_MIN);

  std::unique_ptr<common::Value> v9(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::DECIMAL).GetTypeId(), nullptr));
  EXPECT_EQ(v9->GetTypeId(), common::Type::DECIMAL);
  EXPECT_EQ(v9->GetAs<double>(), common::PELOTON_DECIMAL_MAX);
  std::unique_ptr<common::Value> v10(common::Value::DeserializeFrom(in, common::Type::GetInstance(common::Type::DECIMAL).GetTypeId(), nullptr));
  EXPECT_EQ(v10->GetTypeId(), common::Type::DECIMAL);
  EXPECT_EQ(v10->GetAs<double>(), common::PELOTON_DECIMAL_MIN);
}

}
}

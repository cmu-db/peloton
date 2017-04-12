
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_test.cpp
//
// Identification: /peloton/test/type/value_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "common/exception.h"
#include "storage/tuple.h"
#include "type/serializeio.h"
#include "type/type.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Type Tests
//===--------------------------------------------------------------------===//

class TypeTests : public PelotonTest {};

const std::vector<type::Type::TypeId> typeTestTypes = {
    type::Type::BOOLEAN,   type::Type::TINYINT, type::Type::SMALLINT,
    type::Type::INTEGER,   type::Type::BIGINT,  type::Type::DECIMAL,
    type::Type::TIMESTAMP,
    // type::Type::VARCHAR
};

TEST_F(TypeTests, InvalidTypeTest) {
  // First get the INVALID type instance
  type::Type::TypeId type_id = type::Type::INVALID;
  auto t = type::Type::GetInstance(type_id);
  EXPECT_NE(nullptr, t);
  EXPECT_EQ(type_id, t->GetTypeId());
  EXPECT_FALSE(t->IsCoercableFrom(type_id));

  // Then hit up all of the mofos methods
  // They should all throw exceptions
  EXPECT_THROW(type::Type::GetTypeSize(type_id), peloton::Exception);
  EXPECT_THROW(type::Type::GetMinValue(type_id), peloton::Exception);
  EXPECT_THROW(type::Type::GetMaxValue(type_id), peloton::Exception);

  // All of the Compare methods for an invalid type should throw exceptions too
  auto v0 = type::ValueFactory::GetNullValueByType(type::Type::INTEGER);
  auto v1 = type::ValueFactory::GetNullValueByType(type::Type::INTEGER);
  EXPECT_THROW(t->CompareEquals(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->CompareNotEquals(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->CompareLessThan(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->CompareLessThanEquals(v0, v1),
               peloton::NotImplementedException);
  EXPECT_THROW(t->CompareGreaterThan(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->CompareGreaterThanEquals(v0, v1),
               peloton::NotImplementedException);

  // Same with all of the math functions
  EXPECT_THROW(t->Add(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->Subtract(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->Multiply(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->Divide(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->Modulo(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->Min(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->Max(v0, v1), peloton::NotImplementedException);
  EXPECT_THROW(t->OperateNull(v0, v1), peloton::NotImplementedException);

  // Single argument methods. Same thing. They all drop exception bombs
  EXPECT_THROW(t->Sqrt(v0), peloton::NotImplementedException);
  EXPECT_THROW(t->IsZero(v0), peloton::NotImplementedException);
  EXPECT_THROW(t->IsInlined(v0), peloton::NotImplementedException);
  EXPECT_THROW(t->ToString(v0), peloton::NotImplementedException);
  EXPECT_THROW(t->Hash(v0), peloton::NotImplementedException);
  EXPECT_THROW(t->Copy(v0), peloton::NotImplementedException);
  EXPECT_THROW(t->GetData(v0), peloton::NotImplementedException);
  EXPECT_THROW(t->GetData(nullptr), peloton::NotImplementedException);
  EXPECT_THROW(t->GetLength(v0), peloton::NotImplementedException);

  // Serialization stuff
  peloton::CopySerializeOutput out;
  peloton::CopySerializeInput in(out.Data(), out.Size());
  EXPECT_THROW(t->SerializeTo(v0, nullptr, false, nullptr),
               peloton::NotImplementedException);
  EXPECT_THROW(t->SerializeTo(v0, out), peloton::NotImplementedException);
  EXPECT_THROW(t->DeserializeFrom(nullptr, false, nullptr),
               peloton::NotImplementedException);
  EXPECT_THROW(t->DeserializeFrom(in, nullptr),
               peloton::NotImplementedException);

  // Ok here are the last ones
  size_t size = static_cast<size_t>(123);
  EXPECT_THROW(t->HashCombine(v0, size), peloton::NotImplementedException);
  EXPECT_THROW(t->GetElementAt(v0, 0), peloton::NotImplementedException);
  EXPECT_THROW(t->CastAs(v0, type_id), peloton::NotImplementedException);
  EXPECT_THROW(t->InList(v0, v1), peloton::NotImplementedException);
}

TEST_F(TypeTests, GetInstanceTest) {
  for (auto col_type : typeTestTypes) {
    auto t = type::Type::GetInstance(col_type);
    EXPECT_NE(nullptr, t);
    EXPECT_EQ(col_type, t->GetTypeId());
    EXPECT_TRUE(t->IsCoercableFrom(col_type));
  }
}

TEST_F(TypeTests, MaxValueTest) {
  for (auto col_type : typeTestTypes) {
    auto maxVal = type::Type::GetMaxValue(col_type);
    EXPECT_FALSE(maxVal.IsNull());
    // TODO: We should not be allowed to create a value that is greater than
    // the max value.
  }
}

TEST_F(TypeTests, MinValueTest) {
  for (auto col_type : typeTestTypes) {
    auto minVal = type::Type::GetMinValue(col_type);
    EXPECT_FALSE(minVal.IsNull());
    // TODO: We should not be allowed to create a value that is less than
    // the min value.
  }
}
}
}


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

#include "storage/tuple.h"
#include "type/value.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Value Tests
//===--------------------------------------------------------------------===//

class ValueTests : public PelotonTest {};

const std::vector<type::TypeId> valueTestTypes = {
    type::TypeId::BOOLEAN,   type::TypeId::TINYINT, type::TypeId::SMALLINT,
    type::TypeId::INTEGER,   type::TypeId::BIGINT,  type::TypeId::DECIMAL,
    type::TypeId::TIMESTAMP, type::TypeId::DATE,    type::TypeId::VARCHAR};

TEST_F(ValueTests, HashTest) {
  for (auto col_type : valueTestTypes) {
    auto maxVal = type::Type::GetMaxValue(col_type);
    auto minVal = type::Type::GetMinValue(col_type);

    // Special case for VARCHAR
    if (col_type == type::TypeId::VARCHAR) {
      maxVal = type::ValueFactory::GetVarcharValue(std::string("XXX"), nullptr);
      minVal = type::ValueFactory::GetVarcharValue(std::string("YYY"), nullptr);
    }
    LOG_TRACE("%s => MAX:%s <-> MIN:%s", TypeIdToString(col_type).c_str(),
              maxVal.ToString().c_str(), minVal.ToString().c_str());

    // They should not be equal
    EXPECT_EQ(type::CmpBool::CMP_FALSE, maxVal.CompareEquals(minVal));

    // Nor should their hash values be equal
    auto maxHash = maxVal.Hash();
    auto minHash = minVal.Hash();
    EXPECT_NE(maxHash, minHash);

    // But if I copy the first value then the hashes should be the same
    auto copyVal = maxVal.Copy();
    auto copyHash = copyVal.Hash();
    EXPECT_EQ(type::CmpBool::CMP_TRUE, maxVal.CompareEquals(copyVal));
    EXPECT_EQ(maxHash, copyHash);
  }  // FOR
}

TEST_F(ValueTests, MinMaxTest) {
  for (auto col_type : valueTestTypes) {
    auto maxVal = type::Type::GetMaxValue(col_type);
    auto minVal = type::Type::GetMinValue(col_type);

    // Special case for VARCHAR
    if (col_type == type::TypeId::VARCHAR) {
      maxVal = type::ValueFactory::GetVarcharValue(std::string("AAA"), nullptr);
      minVal = type::ValueFactory::GetVarcharValue(std::string("ZZZ"), nullptr);
      EXPECT_EQ(type::CMP_FALSE, minVal.CompareLessThan(maxVal));
      EXPECT_EQ(type::CMP_FALSE, maxVal.CompareGreaterThan(minVal));
    }

    // FIXME: Broken types!!!
    if (col_type == type::TypeId::VARCHAR) continue;
    LOG_DEBUG("MinMax: %s", TypeIdToString(col_type).c_str());

    // Check that we always get the correct MIN value
    EXPECT_EQ(type::CMP_TRUE, minVal.Min(minVal).CompareEquals(minVal));
    EXPECT_EQ(type::CMP_TRUE, minVal.Min(maxVal).CompareEquals(minVal));
    EXPECT_EQ(type::CMP_TRUE, minVal.Min(minVal).CompareEquals(minVal));

    // Check that we always get the correct MAX value
    EXPECT_EQ(type::CMP_TRUE, maxVal.Max(minVal).CompareEquals(maxVal));
    EXPECT_EQ(type::CMP_TRUE, minVal.Max(maxVal).CompareEquals(maxVal));
    EXPECT_EQ(type::CMP_TRUE, maxVal.Max(minVal).CompareEquals(maxVal));
  } // FOR
}

TEST_F(ValueTests, VarcharCopyTest) {
  std::string str = "hello hello world";

  // Try it once with and without a pool
  for (int i = 0; i < 2; i++) {
    type::AbstractPool *pool = nullptr;
    if (i == 0) {
      pool = TestingHarness::GetInstance().GetTestingPool();
    }
    type::Value val1 = type::ValueFactory::GetVarcharValue(str, pool);
    type::Value val2 = val1.Copy();

    // The Value objects should not be the same
    EXPECT_NE(val1.GetData(), val2.GetData());

    // But their underlying data should be equal
    auto result = val1.CompareEquals(val2);
    EXPECT_EQ(type::CmpBool::CMP_TRUE, result);
  }  // FOR
}
}
}

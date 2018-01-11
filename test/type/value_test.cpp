
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
#include "type/value_factory.h"

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
    auto max_val = type::Type::GetMaxValue(col_type);
    auto min_val = type::Type::GetMinValue(col_type);

    // Special case for VARCHAR
    if (col_type == type::TypeId::VARCHAR) {
      max_val = type::ValueFactory::GetVarcharValue(std::string("XXX"), nullptr);
      min_val = type::ValueFactory::GetVarcharValue(std::string("YYY"), nullptr);
    }
    LOG_TRACE("%s => MAX:%s <-> MIN:%s", TypeIdToString(col_type).c_str(),
              max_val.ToString().c_str(), min_val.ToString().c_str());

    // They should not be equal
    EXPECT_EQ(CmpBool::FALSE, max_val.CompareEquals(min_val));

    // Nor should their hash values be equal
    auto max_hash = max_val.Hash();
    auto min_hash = min_val.Hash();
    EXPECT_NE(max_hash, min_hash);

    // But if I copy the first value then the hashes should be the same
    auto copyVal = max_val.Copy();
    auto copyHash = copyVal.Hash();
    EXPECT_EQ(CmpBool::TRUE, max_val.CompareEquals(copyVal));
    EXPECT_EQ(max_hash, copyHash);
  }  // FOR
}

TEST_F(ValueTests, MinMaxTest) {
  for (auto col_type : valueTestTypes) {
    auto max_val = type::Type::GetMaxValue(col_type);
    auto min_val = type::Type::GetMinValue(col_type);

    // Special case for VARCHAR
    if (col_type == type::TypeId::VARCHAR) {
      max_val = type::ValueFactory::GetVarcharValue(std::string("AAA"), nullptr);
      min_val = type::ValueFactory::GetVarcharValue(std::string("ZZZ"), nullptr);
      EXPECT_EQ(CmpBool::FALSE, min_val.CompareLessThan(max_val));
      EXPECT_EQ(CmpBool::FALSE, max_val.CompareGreaterThan(min_val));
    }

    // FIXME: Broken types!!!
    if (col_type == type::TypeId::VARCHAR) continue;
    LOG_DEBUG("MinMax: %s", TypeIdToString(col_type).c_str());

    // Check that we always get the correct MIN value
    EXPECT_EQ(CmpBool::TRUE, min_val.Min(min_val).CompareEquals(min_val));
    EXPECT_EQ(CmpBool::TRUE, min_val.Min(max_val).CompareEquals(min_val));
    EXPECT_EQ(CmpBool::TRUE, min_val.Min(min_val).CompareEquals(min_val));

    // Check that we always get the correct MAX value
    EXPECT_EQ(CmpBool::TRUE, max_val.Max(min_val).CompareEquals(max_val));
    EXPECT_EQ(CmpBool::TRUE, min_val.Max(max_val).CompareEquals(max_val));
    EXPECT_EQ(CmpBool::TRUE, max_val.Max(min_val).CompareEquals(max_val));
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
    EXPECT_EQ(CmpBool::TRUE, result);
  }  // FOR
}
}
}

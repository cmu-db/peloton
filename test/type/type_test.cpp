
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
    // TODO: We should not be allowed to create a value that is greater than
    // the min value.
  }
}
}
}

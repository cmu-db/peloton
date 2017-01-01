
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

TEST_F(ValueTests, VarcharCopyTest) {
  std::string str = "hello hello world";

  // Try it once with and without a pool
  for (int i = 0; i < 2; i++) {
    type::VarlenPool *pool = nullptr;
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

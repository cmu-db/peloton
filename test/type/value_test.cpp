
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
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  std::string str = "hello hello world";
  type::Value val1 = type::ValueFactory::GetVarcharValue(str, nullptr);
  type::Value val2 = val1.Copy();

  // The Value objects should not be the same
  EXPECT_NE(val1, val2);

  // But their underlying data should be equal
  auto result = val1.CompareEquals(val2);
  EXPECT_EQ(type::CmpBool::CMP_TRUE, result);
}
}
}

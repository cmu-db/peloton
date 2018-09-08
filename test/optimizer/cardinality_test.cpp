//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cardinality_test.cpp
//
// Identification: test/optimizer/cardinality_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer_test_util.cpp"
#include <chrono>


namespace peloton {
namespace test {

class CardinalityTest : public OptimizerTestUtil {};

TEST_F(CardinalityTest, EstimatedCardinalityTest) {

  const std::string test_table_name = "testtable";
  const int num_rows = 10;
  OptimizerTestUtil::CreateTable(test_table_name, num_rows);

  auto plan = GeneratePlan("SELECT a from " + test_table_name + ";");

  EXPECT_EQ(num_rows, plan->GetCardinality());
}

TEST_F(CardinalityTest, EstimatedCardinalityTestWithPredicate) {

  const std::string test_table_name = "testtable";
  const int num_rows = 10;
  OptimizerTestUtil::CreateTable(test_table_name, num_rows);

  auto plan = GeneratePlan("SELECT a from " + test_table_name + " WHERE " + "a < 10;");

  EXPECT_GE(num_rows, plan->GetCardinality());
}

}
}

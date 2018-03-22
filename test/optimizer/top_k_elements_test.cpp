//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// top_k_elements_test.cpp
//
// Identification: test/optimizer/top_k_elements_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "optimizer/stats/count_min_sketch.h"
#include "optimizer/stats/top_k_elements.h"
#include "common/logger.h"
#include "common/statement.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"
#include <string>

namespace peloton {
namespace test {

using namespace optimizer;

class TopKElementsTests : public PelotonTest {};

TEST_F(TopKElementsTests, SimpleArrivalOnlyTest) {
  CountMinSketch sketch(10, 20, 0);

  EXPECT_EQ(sketch.depth, 10);
  EXPECT_EQ(sketch.width, 20);
  EXPECT_EQ(sketch.size, 0);

  // test TopKElements
  const int k = 5;
  TopKElements top_k_elements(sketch, k);
  EXPECT_EQ(top_k_elements.tkq.get_k(), k);
  EXPECT_EQ(top_k_elements.tkq.get_size(), 0);

  top_k_elements.Add(1, 10);
  top_k_elements.Add(2, 5);
  top_k_elements.Add(3, 1);
  top_k_elements.Add(4, 1000000);

  // This case count min sketch should give exact counting
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(1), 10);
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(2), 5);
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(3), 1);
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(4), 1000000);

  // EXPECT_EQ(top_k_elements.cmsketch.size, 4);
  EXPECT_EQ(top_k_elements.tkq.get_size(), 4);

  top_k_elements.Add(5, 15);
  top_k_elements.Add("6", 20);
  top_k_elements.Add("7", 100);
  top_k_elements.Add("8", 1);

  EXPECT_EQ(top_k_elements.tkq.get_size(), 5);
  top_k_elements.PrintTopKQueueOrderedMaxFirst(10);
}

TEST_F(TopKElementsTests, SimpleArrivalAndDepartureTest) {
  CountMinSketch sketch(10, 5, 0);
  EXPECT_EQ(sketch.depth, 10);
  EXPECT_EQ(sketch.width, 5);
  EXPECT_EQ(sketch.size, 0);

  // test TopKElements
  const int k = 5;
  TopKElements top_k_elements(sketch, k);

  top_k_elements.Add("10", 10);
  top_k_elements.Add("5", 5);
  top_k_elements.Add("1", 1);
  top_k_elements.Add("Million", 1000000);

  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount("10"), 10);

  top_k_elements.Add(5, 15);
  top_k_elements.Add("6", 1);
  top_k_elements.Add("7", 2);
  top_k_elements.Add("8", 1);

  EXPECT_EQ(top_k_elements.tkq.get_size(), k);
  top_k_elements.PrintTopKQueueOrderedMaxFirst(10);

  top_k_elements.Remove(5, 14);
  top_k_elements.Remove("10", 20);
  top_k_elements.Remove(100, 10000);
  top_k_elements.PrintTopKQueueOrderedMaxFirst(10);
}

TEST_F(TopKElementsTests, LargeArrivalOnlyTest) {
  CountMinSketch sketch(1000, 1000, 0);

  const int k = 20, num0 = 10;
  TopKElements top_k_elements(sketch, k);

  top_k_elements.Add("10", 10);
  top_k_elements.Add("5", 5);
  top_k_elements.Add("1", 1);
  top_k_elements.Add("Million", 1000000);

  top_k_elements.Add(std::string{"Cowboy"}, 2333);
  top_k_elements.Add(std::string{"Bebop"}, 2334);
  top_k_elements.Add(std::string{"RayCharles"}, 2335);
  int i;
  for (i = 0; i < 30; ++i) {
    top_k_elements.Add(i, i);
  }

  top_k_elements.PrintOrderedMaxFirst(num0);
  EXPECT_EQ(top_k_elements.tkq.get_size(), k);
  EXPECT_EQ(top_k_elements.GetOrderedMaxFirst(num0).size(), num0);
  EXPECT_EQ(top_k_elements.GetAllOrderedMaxFirst().size(), k);

  for (i = 1000; i < 2000; ++i) {
    top_k_elements.Add(i, i);
  }
  top_k_elements.PrintAllOrderedMaxFirst();
}

TEST_F(TopKElementsTests, WrapperTest) {
  CountMinSketch sketch(0.01, 0.1, 0);

  const int k = 5;
  TopKElements top_k_elements(sketch, k);

  type::Value v1 = type::ValueFactory::GetDecimalValue(7.12);
  type::Value v2 = type::ValueFactory::GetDecimalValue(10.25);
  top_k_elements.Add(v1);
  top_k_elements.Add(v2);
  EXPECT_EQ(top_k_elements.GetAllOrderedMaxFirst().size(), 2);

  for (int i = 0; i < 1000; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(4.1525);
    top_k_elements.Add(v);
  }
  EXPECT_EQ(top_k_elements.GetAllOrderedMaxFirst().size(), 3);

  type::Value v3 = type::ValueFactory::GetVarcharValue("luffy");
  type::Value v4 = type::ValueFactory::GetVarcharValue(std::string("monkey"));
  for (int i = 0; i < 500; i++) {
    top_k_elements.Add(v3);
    top_k_elements.Add(v4);
  }
  top_k_elements.PrintAllOrderedMaxFirst();
}

TEST_F(TopKElementsTests, UniformTest) {
  CountMinSketch sketch(0.01, 0.1, 0);

  const int k = 5;
  TopKElements top_k_elements(sketch, k);

  for (int i = 0; i < 1000; i++) {
    type::Value v1 = type::ValueFactory::GetDecimalValue(7.12 + i);
    top_k_elements.Add(v1);
  }
  EXPECT_EQ(top_k_elements.GetAllOrderedMaxFirst().size(), 5);

  top_k_elements.PrintAllOrderedMaxFirst();
}
}
}

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_min_sketch_test.cpp
//
// Identification: test/optimizer/count_min_sketch_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "optimizer/stats/count_min_sketch.h"
#include "common/logger.h"
#include "common/statement.h"

namespace peloton {
namespace test {

using namespace optimizer;

class CountMinSketchTests : public PelotonTest {};

// Basic CM-Sketch testing with double datatype.
TEST_F(CountMinSketchTests, SimpleCountMinSketchIntegerTest) {
  CountMinSketch sketch(10, 20, 0);
  EXPECT_EQ(sketch.depth, 10);
  EXPECT_EQ(sketch.width, 20);
  EXPECT_EQ(sketch.size, 0);

  sketch.Add(1, 10);
  sketch.Add(2, 5);
  sketch.Add(3, 1);
  sketch.Add(4, 1000000);

  // This case count min sketch should give exact counting
  EXPECT_EQ(sketch.EstimateItemCount(1), 10);
  EXPECT_EQ(sketch.EstimateItemCount(2), 5);
  EXPECT_EQ(sketch.EstimateItemCount(3), 1);
  EXPECT_EQ(sketch.EstimateItemCount(4), 1000000);
}

// Basic testing with string datatype.
TEST_F(CountMinSketchTests, SimpleCountMinSketchStringTest) {
  CountMinSketch sketch(10, 5, 0);
  EXPECT_EQ(sketch.depth, 10);
  EXPECT_EQ(sketch.width, 5);
  EXPECT_EQ(sketch.size, 0);

  sketch.Add("10", 10);
  sketch.Add("5", 5);
  sketch.Add("1", 1);
  sketch.Add("Million", 1000000);

  EXPECT_EQ(sketch.EstimateItemCount("10"), 10);
}

TEST_F(CountMinSketchTests, SimpleCountMinSketchMixTest) {
  CountMinSketch sketch(10, 5, 0);
  EXPECT_EQ(sketch.depth, 10);
  EXPECT_EQ(sketch.width, 5);
  EXPECT_EQ(sketch.size, 0);

  sketch.Add(10, 10);
  sketch.Add("5", 5);
  sketch.Add("1", 1);
  sketch.Add("Million", 1000000);
  sketch.Add(100, 35);

  EXPECT_EQ(sketch.EstimateItemCount(10), 10);
  EXPECT_EQ(sketch.size, 5);

  sketch.Remove(50, 35);
  sketch.Remove(100, 40);
  sketch.Remove("1", 3);
  EXPECT_EQ(sketch.size, 3);
}
}
}

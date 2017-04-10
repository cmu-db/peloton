
#include "common/harness.h"

#define private public

#include "optimizer/stats/count_min_sketch.h"
#include "optimizer/stats/top_k_elements.h"
#include "common/logger.h"
#include "common/statement.h"

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
  EXPECT_EQ(top_k_elements.tkq.k, k);
  EXPECT_EQ(top_k_elements.tkq.size, 0);

  top_k_elements.Add(1, 10);
  top_k_elements.Add(2, 5);
  top_k_elements.Add(3, 1);
  top_k_elements.Add(4, 1000000);

  // This case count min sketch should give exact counting
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(1), 10);
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(2), 5);
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(3), 1);
  EXPECT_EQ(top_k_elements.cmsketch.EstimateItemCount(4), 1000000);

  //EXPECT_EQ(top_k_elements.cmsketch.size, 4);
  EXPECT_EQ(top_k_elements.tkq.size, 4);

  top_k_elements.Add(5, 15);
  top_k_elements.Add("6", 20);
  top_k_elements.Add("7", 100);
  top_k_elements.Add("8", 1);
  
  EXPECT_EQ(top_k_elements.tkq.size, 5);
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

  EXPECT_EQ(top_k_elements.tkq.size, k);
  top_k_elements.PrintTopKQueueOrderedMaxFirst(10);

  top_k_elements.Remove(5, 14);
  top_k_elements.Remove("10", 20);
  top_k_elements.Remove(100, 10000);
  top_k_elements.PrintTopKQueueOrderedMaxFirst(10);
}

TEST_F(TopKElementsTests, LargeArrivalOnlyTest) {
  CountMinSketch sketch(10, 20, 0);
  
  const int k = 20;
  TopKElements top_k_elements(sketch, k);
 
  top_k_elements.Add("10", 10);
  top_k_elements.Add("5", 5);
  top_k_elements.Add("1", 1);
  top_k_elements.Add("Million", 1000000);
  int i;
  for (i = 0; i < 30; ++i) {
    top_k_elements.Add(i, i);
  }
  //top_k_elements.PrintTopKQueueOrderedMaxFirst();
  //top_k_elements.PrintTopKQueueOrderedMaxFirst(10);
  top_k_elements.PrintAllOrderedMaxFirst();
  top_k_elements.PrintOrderedMaxFirst(10);
  EXPECT_EQ(top_k_elements.tkq.size, k);
  //top_k_elements.PrintTopKQueuePops();
  //EXPECT_EQ(top_k_elements.tkq.size, 0);
}

}
}

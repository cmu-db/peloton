/*-------------------------------------------------------------------------
 *
 * clusterer_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/tests/brain/clusterer_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cstdio>

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/brain/clusterer.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Clusterer Tests
//===--------------------------------------------------------------------===//

TEST(ClustererTests, BasicTest) {

  oid_t column_count = 5;
  oid_t cluster_count = 2;
  oid_t column_itr;

  brain::Clusterer clusterer(cluster_count, column_count);
  std::vector<double> columns_accessed(column_count, 0);

  double sample_weight = 1;
  for(int sample_itr = 0 ; sample_itr < 10; sample_itr ++) {

    for(column_itr = 0 ; column_itr < column_count ; column_itr++)
      columns_accessed[column_itr] = rand() % 1;

    brain::Sample sample(columns_accessed, sample_weight);
    clusterer.ProcessSample(sample);
  }

  std::cout << clusterer;

}

}  // End test namespace
}  // End peloton namespace

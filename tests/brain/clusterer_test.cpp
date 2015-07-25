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

  brain::Clusterer clusterer(5);

  for(int sample_itr = 0 ; sample_itr < 100000; sample_itr ++) {
    double sample = rand() % 1000;
    clusterer.ProcessSample(sample);
  }

  std::cout << clusterer;

}

}  // End test namespace
}  // End peloton namespace

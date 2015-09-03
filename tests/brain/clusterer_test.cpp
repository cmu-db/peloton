//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// clusterer_test.cpp
//
// Identification: tests/brain/clusterer_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <chrono>

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/brain/clusterer.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Clusterer Tests
//===--------------------------------------------------------------------===//

TEST(ClustererTests, BasicTest) {
  oid_t column_count = 7;
  oid_t cluster_count = 3;

  brain::Clusterer clusterer(cluster_count, column_count);
  std::vector<double> columns_accessed(column_count, 0);
  double sample_weight;

  // initialize a uniform distribution between 0 and 1
  std::mt19937_64 rng;
  std::uniform_real_distribution<double> uniform(0, 1);

  for (int sample_itr = 0; sample_itr < 100; sample_itr++) {
    auto rng_val = uniform(rng);

    if (rng_val < 0.3) {
      columns_accessed = {1, 1, 0, 0, 0, 1, 1};
      sample_weight = 10000;
    } else if (rng_val < 0.6) {
      columns_accessed = {1, 1, 0, 1, 1, 0, 0};
      sample_weight = 1000;
    } else if (rng_val < 0.7) {
      columns_accessed = {0, 0, 1, 1, 1, 0, 0};
      sample_weight = 100;
    } else if (rng_val < 0.8) {
      columns_accessed = {0, 0, 1, 1, 0, 0, 0};
      sample_weight = 100;
    } else {
      columns_accessed = {0, 0, 0, 0, 0, 1, 1};
      sample_weight = 1000;
    }

    brain::Sample sample(columns_accessed, sample_weight);
    clusterer.ProcessSample(sample);
  }

  std::cout << clusterer;

  auto partitioning1 = clusterer.GetPartitioning(2);

  std::cout << "COLUMN "
            << "\t"
            << " TILE"
            << "\n";
  for (auto entry : partitioning1)
    std::cout << entry.first << "\t"
    << entry.second.first << " : " << entry.second.second << "\n";

  auto partitioning2 = clusterer.GetPartitioning(4);
  std::cout << "COLUMN "
            << "\t"
            << " TILE"
            << "\n";
  for (auto entry : partitioning2)
    std::cout << entry.first << "\t"
    << entry.second.first << " : " << entry.second.second << "\n";
}

}  // End test namespace
}  // End peloton namespace

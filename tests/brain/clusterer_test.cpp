//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// clusterer_test.cpp
//
// Identification: tests/brain/clusterer_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <chrono>

#include "harness.h"

#include "backend/brain/clusterer.h"
#include "backend/common/generator.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Clusterer Tests
//===--------------------------------------------------------------------===//

class ClustererTests : public PelotonTest {};

TEST_F(ClustererTests, BasicTest) {
  oid_t column_count = 7;
  oid_t cluster_count = 3;

  brain::Clusterer clusterer(cluster_count, column_count);
  std::vector<double> columns_accessed(column_count, 0);
  double sample_weight;

  // initialize a uniform distribution between 0 and 1
  UniformGenerator generator;

  for (int sample_itr = 0; sample_itr < 100; sample_itr++) {
    auto rng_val = generator.GetSample();

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

  LOG_INFO("%s", clusterer.GetInfo().c_str());

  auto partitioning1 = clusterer.GetPartitioning(2);

  LOG_INFO("COLUMN \t TILE");
  for (auto entry : partitioning1) {
    LOG_INFO("%lu \t %lu : %lu", entry.first, entry.second.first,
             entry.second.second);
  }

  auto partitioning2 = clusterer.GetPartitioning(4);
  LOG_INFO("COLUMN \t TILE");
  for (auto entry : partitioning2) {
    LOG_INFO("%lu \t %lu : %lu", entry.first, entry.second.first,
             entry.second.second);
  }
}

}  // End test namespace
}  // End peloton namespace

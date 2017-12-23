//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// clusterer_test.cpp
//
// Identification: test/brain/clusterer_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <cstdio>
#include <random>
#include <chrono>

#include "common/harness.h"

#include "brain/clusterer.h"
#include "common/generator.h"
#include "util/string_util.h"
#include "util/stringtable_util.h"

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

  LOG_INFO("\n%s", clusterer.GetInfo().c_str());

  auto partitioning1 = clusterer.GetPartitioning(2);

  std::ostringstream os;
  os << "COLUMN\tTILE\n";
  for (UNUSED_ATTRIBUTE auto entry : partitioning1) {
    os << entry.first << "\t" << entry.second.first << " : " << entry.second.second << "\n";
  }
  LOG_INFO("\n%s", peloton::StringUtil::Prefix(peloton::StringTableUtil::Table(os.str(), true),
                                               GETINFO_SPACER).c_str());

  os.str("");
  auto partitioning2 = clusterer.GetPartitioning(4);
  os << "COLUMN\tTILE\n";
  for (UNUSED_ATTRIBUTE auto entry : partitioning2) {
    os << entry.first << "\t" << entry.second.first << " : " << entry.second.second << "\n";
  }
  LOG_INFO("\n%s", peloton::StringUtil::Prefix(peloton::StringTableUtil::Table(os.str(), true),
                                             GETINFO_SPACER).c_str());
}

}  // namespace test
}  // namespace peloton

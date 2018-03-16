//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// clusterer_test.cpp
//
// Identification: test/brain/query_clusterer_test.cpp
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "common/harness.h"
#include "annoy/annoylib.h"
#include "annoy/kissrandom.h"

#include <random>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Clusterer Tests
//===--------------------------------------------------------------------===//

class QueryClustererTests : public PelotonTest {};

TEST_F(QueryClustererTests, BasicTest) {
  int num_features = 40;
  int num_elems = 100000;

  AnnoyIndex<int, double, Angular, Kiss32Random> index =
      AnnoyIndex<int, double, Angular, Kiss32Random>(num_features);

  LOG_INFO("Building index ... be patient !!");

  std::default_random_engine generator;
  std::normal_distribution<double> distribution(0.0, 1.0);

  for (int i = 0; i < num_elems; ++i) {
    double *vec = (double *)malloc(num_features * sizeof(double));

    for (int z = 0; z < num_features; ++z) {
      vec[z] = (distribution(generator));
    }

    index.add_item(i, vec);
  }

  LOG_INFO("Added the values !!");

  index.build(2 * num_features);

  LOG_INFO("Built the tree :D !!");
}

}  // namespace test
}  // namespace peloton

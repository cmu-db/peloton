/*-------------------------------------------------------------------------
 *
 * clusterer.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/brain/clusterer.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "backend/brain/sample.h"
#include "backend/common/types.h"

namespace peloton {
namespace brain {

#define DEFAULT_WEIGHT 0.01

//===--------------------------------------------------------------------===//
// Clusterer
//===--------------------------------------------------------------------===//

// Sequential k-Means Clustering
class Clusterer {
 public:
  Clusterer(oid_t cluster_count,
            oid_t sample_column_count,
            double param = DEFAULT_WEIGHT)
      : cluster_count_(cluster_count),
        means(std::vector<Sample>(cluster_count_, Sample(sample_column_count))),
        new_sample_weight_(param) {

  }

  oid_t GetClusterCount() const { return cluster_count_; }

  // process the sample and update the means
  void ProcessSample(const Sample& sample);

  // find closest cluster for the given sample
  oid_t GetClosestCluster(const Sample& sample) const;

  // get cluster mean sample
  Sample GetCluster(oid_t cluster_offset) const;

  // Get a string representation of clusterer
  friend std::ostream& operator<<(std::ostream& os, const Clusterer& clusterer);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // # of clusters
  oid_t cluster_count_;

  // means
  std::vector<Sample> means;

  // weight for new sample
  double new_sample_weight_;
};

}  // End brain namespace
}  // End peloton namespace

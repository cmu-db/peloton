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
  Clusterer(oid_t class_count, double param = DEFAULT_WEIGHT)
      : cluster_count_(class_count), new_sample_weight_(param) {}

  void SetClusterCount(oid_t cluster_count) {
    cluster_count_ = cluster_count;
    // resize the size of means
    means.resize(cluster_count);
  }

  oid_t GetClusterCount() const { return cluster_count_; }

  // process the sample and update the means
  void ProcessSample(double sample);

  // find closest cluster for the given sample
  oid_t GetClosestCluster(double sample) const;

  // get cluster mean
  double GetCluster(oid_t cluster_offset) const;

  // get the distance between two samples
  double GetDistance(double sample1, double sample2) const;

  // Get a string representation of clusterer
  friend std::ostream& operator<<(std::ostream& os, const Clusterer& clusterer);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // # of clusters
  oid_t cluster_count_;

  // means
  std::vector<double> means;

  // weight for new sample
  double new_sample_weight_;
};

}  // End brain namespace
}  // End peloton namespace

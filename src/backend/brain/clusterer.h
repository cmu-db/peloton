//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// clusterer.h
//
// Identification: src/backend/brain/clusterer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <map>

#include "backend/brain/sample.h"
#include "backend/common/types.h"

namespace peloton {
namespace brain {

#define NEW_SAMPLE_WEIGHT 0.01

//===--------------------------------------------------------------------===//
// Clusterer
//===--------------------------------------------------------------------===//

// Column Id to < Tile Id, Tile Column Id >
typedef std::map<oid_t, std::pair<oid_t, oid_t>> column_map_type;

// Sequential k-Means Clustering
class Clusterer {
 public:
  Clusterer(oid_t cluster_count, oid_t sample_column_count,
            double param = NEW_SAMPLE_WEIGHT)
      : cluster_count_(cluster_count),
        means_(
            std::vector<Sample>(cluster_count_, Sample(sample_column_count))),
        closest_(std::vector<int>(cluster_count_, 0)),
        new_sample_weight_(param),
        sample_count_(0),
        sample_column_count_(sample_column_count) {}

  oid_t GetClusterCount() const { return cluster_count_; }

  // process the sample and update the means
  void ProcessSample(const Sample &sample);

  // find closest cluster for the given sample
  oid_t GetClosestCluster(const Sample &sample);

  // get cluster mean sample
  Sample GetCluster(oid_t cluster_offset) const;

  // get history
  double GetFraction(oid_t cluster_offset) const;

  // get partitioning
  column_map_type GetPartitioning(oid_t tile_count) const;

  // Get a string representation of clusterer
  friend std::ostream &operator<<(std::ostream &os, const Clusterer &clusterer);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // # of clusters
  oid_t cluster_count_;

  // means_
  std::vector<Sample> means_;

  // history
  std::vector<int> closest_;

  // weight for new sample
  double new_sample_weight_;

  // sample count
  int sample_count_;

  // sample column count
  oid_t sample_column_count_;
};

}  // End brain namespace
}  // End peloton namespace

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// kd_tree.h
//
// Identification: src/include/brain/kd_tree.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "annoy/annoylib.h"
#include "annoy/kissrandom.h"
#include "brain/cluster.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// KDTree
//===--------------------------------------------------------------------===//

class KDTree {
 public:
  KDTree(int num_features)
      : num_features_(num_features), index_(num_features) {}

  void Insert(Cluster *cluster) {
    index_.unbuild();
    index_.add_item(size_, cluster->GetCentroid().data());
    index_.build(2 * num_features_);
    cluster->SetIndex(size_);
    clusters_[size_] = cluster;
    size_++;
  }

  void Update(UNUSED_ATTRIBUTE Cluster *cluster) {
    index_.reinitialize();
    Build();
  }

  void GetNN(std::vector<double> &feature, Cluster *&cluster,
             double &similarity) {
    if (size_ == 0) {
      cluster = nullptr;
      similarity = 0.0;
      return;
    }

    std::vector<int> closest;
    std::vector<double> distances;
    index_.get_nns_by_vector(feature.data(), 1, (size_t)-1, &closest,
                             &distances);
    cluster = clusters_[closest[0]];
    similarity = distances[0];
  }

  void Build() {
    for (int i = 0; i < size_; i++) {
      index_.add_item(i, clusters_[i]->GetCentroid().data());
    }
    // n_trees = 2 * num_features, the more the faster, but requires more memory
    index_.build(2 * num_features_);
  }

  void Build(std::set<Cluster *> &clusters) {
    index_.reinitialize();
    clusters_.clear();
    for (auto &cluster : clusters) {
      clusters_.push_back(cluster);
    }
    size_ = clusters_.size();
    Build();
  }

 private:
  int size_;
  int num_features_;
  AnnoyIndex<int, double, Angular, Kiss32Random> index_;
  vector<Cluster *> clusters_;
};

}  // namespace brain
}  // namespace peloton

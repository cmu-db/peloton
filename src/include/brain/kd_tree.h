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

/**
 * KDTree for finding the nearest neigbor of a vector of double of fixed size
 * using angular distance to be used by the Query Clusterer
 */
class KDTree {
 public:
  /**
   * @brief Constructor
   */
  KDTree(int num_features)
      : size_(0), num_features_(num_features), index_(num_features) {}

  /**
   * @brief Insert the cluster into the KDTree
   */
  void Insert(Cluster *cluster);

  /**
   * @brief Update the centroid of the cluster in the index
   */
  void Update(UNUSED_ATTRIBUTE Cluster *cluster);

  /**
   * @brief Get the neares neighbor of the feature in the index
   *
   * @param feature - the vector whose nearest neigbor is being searched for
   * @param cluster - return the cluster of the nearest neighbor in this
   * @param similarity - return the similarity to the nearest neighbor in this
   */
  void GetNN(std::vector<double> &feature, Cluster *&cluster,
             double &similarity);

  /**
   * @brief Reset the clusters and build the index again
   */
  void Build(std::set<Cluster *> &clusters);

 private:
  /**
   * @brief Helper function to build the index from scratch
   */
  void Build();

  // number of (centroid/cluster) entries in the KDTree
  int size_;
  // size of each centroid entry in the KDTree
  int num_features_;
  // similarity search structure for the KDTree
  // - each entry in it is indexed by an int
  // - each entry is a vector of double of fixed size - num_features_
  // - uses Angular Distance metric
  // - Kiss32Random - random number generator
  AnnoyIndex<int, double, Angular, Kiss32Random> index_;
  // clusters whose centroid is in the KDTree
  vector<Cluster *> clusters_;
};

}  // namespace brain
}  // namespace peloton

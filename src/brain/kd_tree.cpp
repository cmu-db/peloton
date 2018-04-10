//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// kd_tree.cpp
//
// Identification: src/brain/kd_tree.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/kd_tree.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

void KDTree::Insert(Cluster *cluster) {
  // TODO[Siva]: Currently we ubuild and the build tree again for every change
  // to the structure. Will need to change AnnoyIndex to optimize this
  index_.unbuild();
  index_.add_item(size_, cluster->GetCentroid().data());
  index_.build(2 * num_features_);
  cluster->SetIndex(size_);
  clusters_.push_back(cluster);
  size_++;
}

// TODO[Siva]: cluster is unused as the index is rebuilt using the centroids
// of all the existing clusters already existing in the clusters_
void KDTree::Update(UNUSED_ATTRIBUTE Cluster *cluster) {
  // TODO[Siva]: Currently we ubuild and the build tree again for every change
  // to the structure. Will need to change AnnoyIndex to optimize this
  // The update to the centroid is reflected in the cluster. There is no change
  // to the clusters_, so just rebuild the entire index
  index_.unload();
  Build();
}

void KDTree::GetNN(std::vector<double> &feature, Cluster *&cluster,
                   double &similarity) {
  if (size_ == 0) {
    cluster = nullptr;
    similarity = 0.0;
    return;
  }

  std::vector<int> closest;
  std::vector<double> distances;
  index_.get_nns_by_vector(feature.data(), 1, (size_t)-1, &closest, &distances);
  cluster = clusters_[closest[0]];
  // convert the angular distance to corresponsing cosine similarity
  similarity = (2.0 - distances[0]) / 2.0;
}

void KDTree::Build() {
  for (int i = 0; i < size_; i++) {
    index_.add_item(i, clusters_[i]->GetCentroid().data());
  }
  // number of random forests built by the AnnoyIndex = 2 * num_features
  // the more the faster, but requires more memory
  index_.build(2 * num_features_);
}

void KDTree::Build(std::set<Cluster *> &clusters) {
  index_.unload();
  clusters_.clear();
  for (auto &cluster : clusters) {
    clusters_.push_back(cluster);
  }
  size_ = clusters_.size();
  Build();
}

}  // namespace brain
}  // namespace peloton

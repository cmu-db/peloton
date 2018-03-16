//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_clusterer.h
//
// Identification: src/include/brain/query_clusterer.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <set>
#include <string>
#include <vector>

#include "brain/cluster.h"
#include "brain/kd_tree.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// QueryClusterer
//===--------------------------------------------------------------------===//

class QueryClusterer {
 public:
  QueryClusterer(int num_features) : threshold_(0.7), kd_tree_(num_features) {}

  // Collect the latest period's data from the server and update the feature
  // vectors of the template queries
  void UpdateFeatures();

  void UpdateTemplate(std::string fingerprint, bool is_new);

  void UpdateExistingTemplates();

  void MergeClusters();

  void UpdateCluster();

 private:
  // Map from the fingerprint of the template query to its feature vector
  std::map<std::string, std::vector<double>> features_;
  std::map<std::string, Cluster *> template_cluster_;
  std::set<Cluster *> clusters_;

  double threshold_;
  int num_features_;

  // Similarity search structure for finding the nearest centroid;
  KDTree kd_tree_;
};

}  // namespace brain
}  // namespace peloton

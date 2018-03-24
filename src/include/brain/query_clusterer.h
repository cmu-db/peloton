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
  /**
   * @brief Constructor
   */
  QueryClusterer(int num_features, double threshold)
      : num_features_(num_features),
        threshold_(threshold),
        kd_tree_(num_features) {}

  /**
   * @brief Collects the latest data from the server, update the feature
   * vectors of each template and insert the new templates into the clusters
   */
  void UpdateFeatures();

  /**
   * @brief Update the cluster of the given template
   *
   * @param fingerprint - the fingerprint of the template whose cluster needs
   * to be updated
   * @param is_new - true if the fingerprint is seen for the first time
   */
  void UpdateTemplate(std::string fingerprint, bool is_new);

  /**
   * @brief Update the cluster of the existing templates in each cluster
   */
  void UpdateExistingTemplates();

  /**
   * @brief Merge the clusters that are within the threshold distance
   */
  void MergeClusters();

  /**
   * @brief Update the clusters for the current time period
   * This functions needs to be called by the scheduler at the end of every
   * period
   */
  void UpdateCluster();

  /**
   * @brief Add a feature (new/existing) into the cluster
   */
  void AddFeature(std::string &fingerprint, std::vector<double> feature);

  /**
   * @brief Return the all the clusters
   */
  const std::set<Cluster *> &GetClusters() { return clusters_; }

  /**
   * @brief Destroyer
   */
  ~QueryClusterer();

 private:
  // Map from the fingerprint of the template query to its feature vector
  std::map<std::string, std::vector<double>> features_;
  // Map from the fingerprint of the template query to its cluster
  std::map<std::string, Cluster *> template_cluster_;
  // Set of all clusters
  std::set<Cluster *> clusters_;
  // number of features or size of each feature vector
  int num_features_;
  // the threshold on similarity of feature vectors
  double threshold_;
  // KDTree for finding the nearest cluster
  KDTree kd_tree_;
};

}  // namespace brain
}  // namespace peloton

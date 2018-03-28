//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cluster.h
//
// Identification: src/include/brain/cluster.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// Cluster
//===--------------------------------------------------------------------===//

class Cluster {
 public:
  Cluster(int num_features) : centroid_(num_features, 0.0) {}

  /**
   * @brief Add the fingerprint to the set of templates of the cluster
   * and update the centroid of the cluster
   */
  void AddTemplateAndUpdateCentroid(std::string &fingerprint,
                                    std::vector<double> &feature);

  /**
   * @brief Add the fingerprint to the set of templates without updating the
   * centroid
   */
  void AddTemplate(const std::string &fingerprint);

  /**
   * @brief Remove the fingerprint from the set of templates without updating
   * the centroid
   */
  void RemoveTemplate(std::string &fingerprint);

  /**
   * @brief Update the centroid of the cluster
   *
   * @param features : map from the fingerprint to its feature vector
   */
  void UpdateCentroid(std::map<std::string, std::vector<double>> &features);

  /**
   * @brief Compute the cosine similarity between the centroid of the cluster
   * and the given feature vector
   */
  double CosineSimilarity(std::vector<double> &feature);

  /**
   * @brief Return the index of the cluster in the KDTree
   */
  int GetIndex() { return index_; }

  /**
   * @brief Set the index of the cluster, set by the KDTree
   */
  void SetIndex(int index) { index_ = index; }

  /**
   * @brief Return the number of fingerprints in the cluster
   */
  int GetSize() { return templates_.size(); }

  /**
   * @brief Return the centroid of the cluster
   */
  std::vector<double> GetCentroid() { return centroid_; }

  /**
   * @brief Return the fingerprints in the cluster
   */
  const std::set<std::string> &GetTemplates() { return templates_; }

 private:
  // index of the cluster in the KDTree
  uint32_t index_;
  // centroid of the cluster
  std::vector<double> centroid_;
  // fingerprints in the cluster
  std::set<std::string> templates_;
};

}  // namespace brain
}  // namespace peloton

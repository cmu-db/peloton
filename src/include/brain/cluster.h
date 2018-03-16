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

#include "common/macros.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// Cluster
//===--------------------------------------------------------------------===//

class Cluster {
 public:
  Cluster(int num_features) : centroid_(num_features, 0.0) {}

  void AddTemplateAndUpdateCentroid(std::string &fingerprint,
                                    std::vector<double> &feature);

  void AddTemplate(const std::string &fingerprint);

  void RemoveTemplate(std::string &fingerprint);

  void UpdateCentroid(std::map<std::string, std::vector<double>> &features);

  double CosineSimilarity(std::vector<double> &feature);

  int GetIndex() { return index_; }

  void SetIndex(int index) { index_ = index; }

  int GetSize() { return templates_.size(); }

  std::vector<double> GetCentroid() { return centroid_; }

  std::set<std::string> GetTemplates() { return templates_; }

 private:
  uint32_t index_;
  std::vector<double> centroid_;
  std::set<std::string> templates_;
};

}  // namespace brain
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cluster.cpp
//
// Identification: src/brain/cluster.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/cluster.h"
#include "common/logger.h"
#include "common/macros.h"

namespace peloton {
namespace brain {

void Cluster::AddTemplateAndUpdateCentroid(std::string &fingerprint,
                                           std::vector<double> &feature) {
  auto num_templates = templates_.size();
  for (unsigned int i = 0u; i < feature.size(); i++) {
    centroid_[i] +=
        (centroid_[i] * num_templates + feature[i]) * 1.0 / (num_templates + 1);
  }
  templates_.insert(fingerprint);
}

void Cluster::AddTemplate(const std::string &fingerprint) {
  templates_.insert(fingerprint);
}

void Cluster::RemoveTemplate(std::string &fingerprint) {
  templates_.erase(fingerprint);
}

void Cluster::UpdateCentroid(
    std::map<std::string, std::vector<double>> &features) {
  int num_features = centroid_.size();
  std::fill(centroid_.begin(), centroid_.end(), 0);
  PELOTON_ASSERT(templates_.size() != 0);

  for (auto fingerprint : templates_) {
    auto feature = features[fingerprint];
    for (int i = 0; i < num_features; i++) {
      centroid_[i] += feature[i];
    }
  }

  for (int i = 0; i < num_features; i++) {
    centroid_[i] /= (templates_.size());
  }
}

double Cluster::CosineSimilarity(std::vector<double> &feature) {
  double dot = 0.0, denom_a = 0.0, denom_b = 0.0;
  double epsilon = 1e-5;
  for (unsigned int i = 0u; i < feature.size(); i++) {
    dot += centroid_[i] * feature[i];
    denom_a += centroid_[i] * centroid_[i];
    denom_b += feature[i] * feature[i];
  }

  if (denom_a < epsilon || denom_b < epsilon) return 0.0;

  return dot / (sqrt(denom_a) * sqrt(denom_b));
}

}  // namespace brain
}  // namespace peloton

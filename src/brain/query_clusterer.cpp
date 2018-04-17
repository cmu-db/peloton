//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_clusterer.cpp
//
// Identification: src/brain/query_clusterer.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/query_clusterer.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

void QueryClusterer::UpdateFeatures() {
  // Read the latest queries from server over RPC or the brainside catalog
  // Update the feature vectors for template queries and l2 - normalize them
  // For new templates - insert into templates_ and
  // call UpdateTemplate(fingerprint, true)
}

void QueryClusterer::UpdateTemplate(std::string fingerprint, bool is_new) {
  // Find the nearest cluster of the template's feature vector by querying the
  // KDTree of the centroids of the clusters. If the similarity of the feature
  // with the cluster is greater than the threshold, add it to the cluster.
  // Otherwise create a new cluster with this template
  auto feature = features_[fingerprint];
  double similarity = 0.0;
  Cluster *cluster = nullptr;

  kd_tree_.GetNN(feature, cluster, similarity);

  if (cluster == nullptr) {
    // If the kd_tree_ is empty
    cluster = new Cluster(num_features_);
    cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
    kd_tree_.Insert(cluster);
    clusters_.insert(cluster);
    template_cluster_[fingerprint] = cluster;
    return;
  }

  if (similarity > threshold_) {
    // If the nearest neighbor has a similarity higher than the threshold_
    if (is_new) {
      cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
      kd_tree_.Update(cluster);
    } else {
      // updating an existing template, so need not update the centroid
      cluster->AddTemplate(fingerprint);
    }
  } else {
    // create a new cluster as the nearest neighbor is not similar enough
    cluster = new Cluster(num_features_);
    cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
    kd_tree_.Insert(cluster);
    clusters_.insert(cluster);
  }

  template_cluster_[fingerprint] = cluster;
}

void QueryClusterer::UpdateExistingTemplates() {
  // for each template check the similarity with the cluster
  // if the similarity is less than the threshold, then remove it
  // and insert into the next nearest cluster
  // Update the centroids at the end of the round only
  for (auto &feature : features_) {
    auto fingerprint = feature.first;
    auto *cluster = template_cluster_[fingerprint];
    auto similarity = cluster->CosineSimilarity(feature.second);
    if (similarity < threshold_) {
      cluster->RemoveTemplate(fingerprint);
      UpdateTemplate(fingerprint, false);
    }
  }

  std::vector<Cluster *> to_delete;
  for (auto &cluster : clusters_) {
    if (cluster->GetSize() == 0) {
      to_delete.push_back(cluster);
    } else {
      cluster->UpdateCentroid(features_);
    }
  }

  // Delete the clusters that are empty
  for (auto cluster : to_delete) {
    clusters_.erase(cluster);
    delete cluster;
  }

  // Rebuild the tree to account for the deleted clusters
  kd_tree_.Build(clusters_);
}

void QueryClusterer::MergeClusters() {
  // Merge two clusters that are within the threshold in similarity
  // Iterate from left to right and merge the left one into right one and mark
  // the left one for deletion
  std::vector<Cluster *> to_delete;
  for (auto i = clusters_.begin(); i != clusters_.end(); i++) {
    for (auto j = i; ++j != clusters_.end();) {
      auto left = *i;
      auto right = *j;
      auto r_centroid = right->GetCentroid();
      auto similarity = left->CosineSimilarity(r_centroid);

      if (similarity > threshold_) {
        auto templates = left->GetTemplates();
        for (auto &fingerprint : templates) {
          right->AddTemplate(fingerprint);
          template_cluster_[fingerprint] = right;
        }
        right->UpdateCentroid(features_);
        to_delete.push_back(left);
        break;
      }
    }
  }

  // Delete the clusters that are empty
  for (auto cluster : to_delete) {
    clusters_.erase(cluster);
    delete cluster;
  }

  // Rebuild the KDTree to account for changed clusters
  kd_tree_.Build(clusters_);
}

void QueryClusterer::UpdateCluster() {
  // This function needs to be scheduled periodically for updating the clusters
  // Update the feature vectors of all templates, update new and existing
  // templates and merge the clusters
  UpdateFeatures();
  UpdateExistingTemplates();
  MergeClusters();
}

void QueryClusterer::AddFeature(std::string &fingerprint,
                                std::vector<double> feature) {
  // Normalize and add a feature into the cluster.
  // This is currently used only for testing.
  double l2_norm = 0.0;
  for (uint i = 0; i < feature.size(); i++) l2_norm += feature[i] * feature[i];

  if (l2_norm > 0.0)
    for (uint i = 0; i < feature.size(); i++) feature[i] /= l2_norm;

  if (features_.find(fingerprint) == features_.end()) {
    // Update the cluster if it's a new template
    features_[fingerprint] = feature;
    UpdateTemplate(fingerprint, true);
  } else {
    features_[fingerprint] = feature;
  }
}

QueryClusterer::~QueryClusterer() {
  for (auto &cluster : clusters_) delete cluster;
}

}  // namespace brain
}  // namespace peloton

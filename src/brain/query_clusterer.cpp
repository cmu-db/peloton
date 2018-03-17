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

namespace peloton {
namespace brain {

void QueryClusterer::UpdateFeatures() {
  /*
   * Read the latest queries from server over RPC
   * Update the feature vectors for template queries
   * For new templates - insert into templates_ and
   * call UpdateTemplate(fingerprint, true)
   */
}

void QueryClusterer::UpdateTemplate(std::string fingerprint, bool is_new) {
  auto feature = features_[fingerprint];
  double similarity = 0.0;
  Cluster *cluster = nullptr;
  // TODO[Siva]: Ensure that this doesn't fail when the KDTree is empty
  kd_tree_.GetNN(feature, cluster, similarity);

  if (cluster == nullptr) {
    cluster = new Cluster(num_features_);
    cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
    kd_tree_.Insert(cluster);
    clusters_.insert(cluster);
    template_cluster_[fingerprint] = cluster;
    return;
  }

  if (similarity > threshold_) {
    if (is_new) {
      cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
      kd_tree_.Update(cluster);
    } else
      cluster->AddTemplate(fingerprint);
  } else {
    cluster = new Cluster(num_features_);
    cluster->AddTemplateAndUpdateCentroid(fingerprint, feature);
    kd_tree_.Insert(cluster);
    clusters_.insert(cluster);
  }
  template_cluster_[fingerprint] = cluster;
}

void QueryClusterer::UpdateExistingTemplates() {
  // for each template check the similarity with the cluster
  // if the distance is less than the threshold, then remove it and insert into
  // the next nearest cluster. Don't update the centroids

  for (auto &feature : features_) {
    auto fingerprint = feature.first;
    auto *cluster = template_cluster_[fingerprint];
    auto similarity = cluster->CosineSimilarity(feature.second);

    if (similarity < threshold_) {
      cluster->RemoveTemplate(fingerprint);

      if (cluster->GetSize() == 0) {
        clusters_.erase(cluster);
        // TODO[Siva]: This line can be avoided
        template_cluster_.erase(fingerprint);
        delete cluster;
      }

      UpdateTemplate(fingerprint, false);
    }
  }

  // Update the centroids for all the clusters after this;
  for (auto &cluster : clusters_) {
    cluster->UpdateCentroid(features_);
  }
}

void QueryClusterer::MergeClusters() {
  for (auto i = clusters_.begin(); i != clusters_.end(); i++) {
    for (auto j = i; ++j != clusters_.end();) {
      auto left = *i;
      auto right = *j;
      auto r_centroid = right->GetCentroid();
      auto similarity = left->CosineSimilarity(r_centroid);

      if (similarity > threshold_) {
        auto templates = right->GetTemplates();
        for (auto &fingerprint : templates) {
          left->AddTemplate(fingerprint);
          template_cluster_[fingerprint] = left;
        }
        left->UpdateCentroid(features_);
        clusters_.erase(right);
      }
    }
  }

  kd_tree_.Build(clusters_);
}

void QueryClusterer::UpdateCluster() {
  UpdateFeatures();
  UpdateExistingTemplates();
  MergeClusters();
}

QueryClusterer::~QueryClusterer() {
  for (auto &cluster : clusters_) delete cluster;
}

}  // namespace brain
}  // namespace peloton

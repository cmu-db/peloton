//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_clusterer_test.cpp
//
// Identification: test/brain/query_clusterer_test.cpp
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <random>

#include "common/harness.h"
#include "brain/query_clusterer.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Clusterer Tests
//===--------------------------------------------------------------------===//

// Function to check if the cluster has same fingerprints as expected
bool ClusterCorrectness(std::set<brain::Cluster *> clusters,
                        std::vector<std::vector<std::string>> expected) {
  std::vector<std::vector<std::string>> actual;
  for (auto cluster : clusters) {
    auto templates = cluster->GetTemplates();
    std::vector<std::string> fingerprints;
    for (auto fingerprint : templates) {
      fingerprints.push_back(fingerprint);
    }
    actual.push_back(fingerprints);
  }

  for (uint i = 0; i < expected.size(); i++) {
    sort(expected[i].begin(), expected[i].end());
  }

  sort(expected.begin(), expected.end());
  sort(actual.begin(), actual.end());

  bool check = (expected.size() == actual.size());
  for (uint i = 0; check && i < expected.size(); i++) {
    check = check && (expected[i].size() == actual[i].size());
    check = check && std::equal(expected[i].begin(), expected[i].end(),
                                actual[i].begin());
  }

  return check;
}

class QueryClustererTests : public PelotonTest {};

TEST_F(QueryClustererTests, ClusterTest) {
  int num_features = 5;
  double threshold = 0.7;
  brain::QueryClusterer query_clusterer(num_features, threshold);

  std::vector<std::string> fingerprints = {"f0", "f1", "f2", "f3", "f4", "f5"};
  std::vector<std::vector<double>> features = {
      {1, 2, 0, 0, 0}, {2, 4, 0, 0, 0}, {1, 0, 0, 0, 7},
      {0, 0, 1, 2, 3}, {2, 0, 0, 0, 8}, {0, 0, 3, 6, 10}};

  // Test whether new templates are clustered properly
  for (uint i = 0; i < 2; i++) {
    query_clusterer.AddFeature(fingerprints[i], features[i]);
  }

  std::set<brain::Cluster *> clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 1);
  EXPECT_TRUE(
      ClusterCorrectness(clusters, {{fingerprints[0], fingerprints[1]}}));

  for (uint i = 2; i < 4; i++) {
    query_clusterer.AddFeature(fingerprints[i], features[i]);
  }

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 3);
  EXPECT_TRUE(ClusterCorrectness(clusters, {{fingerprints[0], fingerprints[1]},
                                            {fingerprints[2]},
                                            {fingerprints[3]}}));

  for (uint i = 4; i < 6; i++) {
    query_clusterer.AddFeature(fingerprints[i], features[i]);
  }

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 3);
  EXPECT_TRUE(
      ClusterCorrectness(clusters, {{fingerprints[0], fingerprints[1]},
                                    {fingerprints[2], fingerprints[4]},
                                    {fingerprints[3], fingerprints[5]}}));

  // Test whether existing templates are re-clustered properly
  features[3] = {100, 200, 0, 0, 0};
  query_clusterer.AddFeature(fingerprints[3], features[3]);
  query_clusterer.UpdateExistingTemplates();

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 3);
  EXPECT_TRUE(ClusterCorrectness(
      clusters, {{fingerprints[0], fingerprints[1], fingerprints[3]},
                 {fingerprints[2], fingerprints[4]},
                 {fingerprints[5]}}));

  // Multiple features are moved, but cluster centroids are not updated until
  // the round of UpdateExistingTemplates() is over
  features[2] = {0, 0, 4, 9, 13};
  features[4] = {5, 10, 0, 0, 0};
  features[5] = {10, 0, 0, 0, 70};
  query_clusterer.AddFeature(fingerprints[2], features[2]);
  query_clusterer.AddFeature(fingerprints[4], features[4]);
  query_clusterer.AddFeature(fingerprints[5], features[5]);
  query_clusterer.UpdateExistingTemplates();

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 3);
  EXPECT_TRUE(ClusterCorrectness(clusters, {{fingerprints[0], fingerprints[1],
                                             fingerprints[3], fingerprints[4]},
                                            {fingerprints[2]},
                                            {fingerprints[5]}}));

  // A new cluster gets added
  features[4] = {0, 10, 0, 10, 0};
  query_clusterer.AddFeature(fingerprints[4], features[4]);
  query_clusterer.UpdateExistingTemplates();

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 4);
  EXPECT_TRUE(ClusterCorrectness(
      clusters, {{fingerprints[0], fingerprints[1], fingerprints[3]},
                 {fingerprints[2]},
                 {fingerprints[4]},
                 {fingerprints[5]}}));

  // A cluster gets deleted
  features[2] = {0, 1, 0, 1, 0};
  query_clusterer.AddFeature(fingerprints[2], features[2]);
  query_clusterer.UpdateExistingTemplates();

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 3);
  EXPECT_TRUE(ClusterCorrectness(
      clusters, {{fingerprints[0], fingerprints[1], fingerprints[3]},
                 {fingerprints[2], fingerprints[4]},
                 {fingerprints[5]}}));

  // Test whether clusters are merged properly
  features[2] = {2, 6, 0, 4, 0};
  features[4] = {2, 6, 0, 4, 0};
  features[5] = {2, 6, 0, 4, 0};
  query_clusterer.AddFeature(fingerprints[2], features[2]);
  query_clusterer.AddFeature(fingerprints[4], features[4]);
  query_clusterer.AddFeature(fingerprints[5], features[5]);
  query_clusterer.UpdateExistingTemplates();

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 2);
  EXPECT_TRUE(ClusterCorrectness(
      clusters, {{fingerprints[0], fingerprints[1], fingerprints[3]},
                 {fingerprints[2], fingerprints[4], fingerprints[5]}}));

  query_clusterer.MergeClusters();

  clusters = query_clusterer.GetClusters();
  EXPECT_EQ(clusters.size(), 1);
  EXPECT_TRUE(ClusterCorrectness(
      clusters, {{fingerprints[0], fingerprints[1], fingerprints[2],
                  fingerprints[3], fingerprints[4], fingerprints[5]}}));
}
}  // namespace test
}  // namespace peloton

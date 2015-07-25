/*-------------------------------------------------------------------------
 *
 * clusterer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/brain/clusterer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <limits>
#include <sstream>
#include <iostream>

#include "backend/brain/clusterer.h"

namespace peloton {
namespace brain {

// http://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm
void Clusterer::ProcessSample(const Sample& sample) {
  // Figure out closest cluster
  oid_t closest_cluster = GetClosestCluster(sample);

  double distance = sample.GetDistance(means[closest_cluster], false);
  double mean_drift = new_sample_weight_ * distance;

  // Update the cluster's mean
  means[closest_cluster] =  means[closest_cluster] + mean_drift;

}

oid_t Clusterer::GetClosestCluster(const Sample& sample) const {
  double min_dist = std::numeric_limits<double>::max();
  oid_t closest_cluster = START_OID;
  oid_t cluster_itr = START_OID;

  // Go over all the means and find closest cluster
  for (auto mean : means) {
    auto dist = sample.GetDistance(mean, true);
    if (dist < min_dist) {
      closest_cluster = cluster_itr;
      min_dist = dist;
    }
    cluster_itr++;
  }

  return closest_cluster;
}

Sample Clusterer::GetCluster(oid_t cluster_offset) const {
  return means[cluster_offset];
}

std::ostream &operator<<(std::ostream &os, const Clusterer &clusterer) {
  oid_t cluster_itr;
  oid_t cluster_count;

  cluster_count = clusterer.GetClusterCount();

  os << "Cluster count : " << cluster_count << "\n";

  for (cluster_itr = 0; cluster_itr < cluster_count; cluster_itr++)
    os << cluster_itr << " : " << clusterer.GetCluster(cluster_itr) << "\n";

  return os;
}

}  // End brain namespace
}  // End peloton namespace

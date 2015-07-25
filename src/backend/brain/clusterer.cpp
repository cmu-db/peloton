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
#include <cmath>
#include <iostream>

#include "backend/brain/clusterer.h"

namespace peloton {
namespace brain {

// http://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm
void Clusterer::ProcessSample(double sample) {
  // Figure out closest cluster
  oid_t closest_cluster = GetClosestCluster(sample);

  // Update the cluster's mean
  means[closest_cluster] +=
      new_sample_weight_ * (sample - means[closest_cluster]);
}

oid_t Clusterer::GetClosestCluster(double sample) const {
  double min_dist = std::numeric_limits<double>::max();
  oid_t closest_cluster = START_OID;
  oid_t cluster_itr = START_OID;

  // Go over all the means and find closest cluster
  for (auto mean : means) {
    auto dist = GetDistance(sample, mean);
    if (dist < min_dist) {
      closest_cluster = cluster_itr;
      min_dist = dist;
    }
    cluster_itr++;
  }

  return closest_cluster;
}

double Clusterer::GetCluster(oid_t cluster_offset) const {
  return means[cluster_offset];
}

double Clusterer::GetDistance(double sample1, double sample2) const {
  return std::abs(sample1 - sample2);
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

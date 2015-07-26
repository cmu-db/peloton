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

  Sample distance = sample.GetDifference(means_[closest_cluster]);
  Sample mean_drift = distance * new_sample_weight_;

  //std::cout << "mean drift : " << mean_drift << "\n";

  // Update the cluster's mean
  means_[closest_cluster] =  means_[closest_cluster] + mean_drift;

}

oid_t Clusterer::GetClosestCluster(const Sample& sample) {
  double min_dist = std::numeric_limits<double>::max();
  oid_t closest_cluster = START_OID;
  oid_t cluster_itr = START_OID;

  // Go over all the means and find closest cluster
  for (auto mean : means_) {
    auto dist = sample.GetDistance(mean);
    if (dist < min_dist) {
      closest_cluster = cluster_itr;
      min_dist = dist;
    }
    cluster_itr++;
  }

  closest_[closest_cluster]++;
  sample_count_++;

  return closest_cluster;
}

Sample Clusterer::GetCluster(oid_t cluster_offset) const {
  return means_[cluster_offset];
}

double Clusterer::GetFraction(oid_t cluster_offset) const {
  return ((double) closest_[cluster_offset])/sample_count_;
}

std::ostream &operator<<(std::ostream &os, const Clusterer &clusterer) {
  oid_t cluster_itr;
  oid_t cluster_count;

  cluster_count = clusterer.GetClusterCount();
  for (cluster_itr = 0; cluster_itr < cluster_count; cluster_itr++)
    os << cluster_itr << " : " << clusterer.GetFraction(cluster_itr)
    << " :: " << clusterer.GetCluster(cluster_itr);

  return os;
}

}  // End brain namespace
}  // End peloton namespace

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// clusterer.cpp
//
// Identification: src/brain/clusterer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <limits>
#include <sstream>
#include <iostream>
#include <map>

#include "brain/clusterer.h"
#include "common/logger.h"
#include "common/macros.h"
#include "util/string_util.h"

namespace peloton {
namespace brain {

// http://www.cs.princeton.edu/courses/archive/fall08/cos436/Duda/C/sk_means.htm
void Clusterer::ProcessSample(const Sample &sample) {
  // Figure out closest cluster
  oid_t closest_cluster = GetClosestCluster(sample);

  Sample distance = sample.GetDifference(means_[closest_cluster]);
  Sample mean_drift = distance * new_sample_weight_;

  // Update the cluster's mean
  means_[closest_cluster] = means_[closest_cluster] + mean_drift;
}

oid_t Clusterer::GetClosestCluster(const Sample &sample) {
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
  return ((double)closest_[cluster_offset]) / sample_count_;
}

column_map_type Clusterer::GetPartitioning(oid_t tile_count) const {
  PL_ASSERT(tile_count >= 1);
  PL_ASSERT(tile_count <= sample_column_count_);

  std::map<double, oid_t> frequencies;
  oid_t cluster_itr = START_OID;
  oid_t cluster_count;

  cluster_count = GetClusterCount();
  for (cluster_itr = 0; cluster_itr < cluster_count; cluster_itr++) {
    auto pair = std::make_pair(GetFraction(cluster_itr), cluster_itr);
    frequencies.insert(pair);
  }

  std::map<oid_t, oid_t> column_to_tile_map;
  oid_t tile_itr = START_OID;
  oid_t remaining_column_count = sample_column_count_;

  // look for most significant cluster
  for (auto entry = frequencies.rbegin(); entry != frequencies.rend();
       ++entry) {
    LOG_TRACE(" %u :: %.3lf", entry->second, entry->first);

    // first, check if remaining columns less than tile count
    if (remaining_column_count <= tile_count) {
      oid_t column_itr;
      for (column_itr = 0; column_itr < sample_column_count_; column_itr++) {
        if (column_to_tile_map.count(column_itr) == 0) {
          column_to_tile_map[column_itr] = tile_itr;
          tile_itr++;
        }
      }
    }

    // otherwise, get its partitioning
    auto config = means_[entry->second];
    auto config_tile = config.GetEnabledColumns();

    for (auto column : config_tile) {
      if (column_to_tile_map.count(column) == 0) {
        column_to_tile_map[column] = tile_itr;
        remaining_column_count--;
      }
    }

    // check tile itr
    tile_itr++;
    if (tile_itr >= tile_count) tile_itr--;
  }

  // check if all columns are present in partitioning
  PL_ASSERT(column_to_tile_map.size() == sample_column_count_);

  // build partitioning
  column_map_type partitioning;
  std::map<oid_t, oid_t> tile_column_count_map;

  for (auto entry : column_to_tile_map) {
    auto column_id = entry.first;
    auto tile_id = entry.second;

    // figure out how many columns in given tile
    auto exists = tile_column_count_map.find(tile_id);
    if (exists == tile_column_count_map.end())
      tile_column_count_map[tile_id] = 0;
    else
      tile_column_count_map[tile_id] += 1;

    // create an entry for the partitioning map
    auto partition_entry =
        std::make_pair(tile_id, tile_column_count_map[tile_id]);
    partitioning[column_id] = partition_entry;
  }

  return partitioning;
}

const std::string Clusterer::GetInfo() const {
  std::ostringstream os;

  oid_t cluster_itr;
  oid_t cluster_count;

  cluster_count = GetClusterCount();
  for (cluster_itr = 0; cluster_itr < cluster_count; cluster_itr++)
    os << cluster_itr << " : " << GetFraction(cluster_itr)
       << " :: " << GetCluster(cluster_itr) << std::endl;
  std::string info = os.str();
  peloton::StringUtil::RTrim(info);
  return info;
}

}  // namespace brain
}  // namespace peloton

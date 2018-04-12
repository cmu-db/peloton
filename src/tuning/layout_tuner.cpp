//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_tuner.cpp
//
// Identification: src/tuning/layout_tuner.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "tuning/layout_tuner.h"

#include <vector>
#include <string>
#include <algorithm>

#include "catalog/schema.h"
#include "common/logger.h"
#include "common/timer.h"
#include "storage/data_table.h"

namespace peloton {
namespace tuning {

LayoutTuner& LayoutTuner::GetInstance() {
  static LayoutTuner layout_tuner;
  return layout_tuner;
}

LayoutTuner::LayoutTuner() {
  // Nothing to do here !
}

LayoutTuner::~LayoutTuner() {}

void LayoutTuner::Start() {
  // Set signal
  layout_tuning_stop = false;

  // Launch thread
  layout_tuner_thread = std::thread(&tuning::LayoutTuner::Tune, this);

  LOG_INFO("Started layout tuner");
}

/**
 * @brief Print information from column map, used to inspect layout generated
 * from clusterer.
 *
 * @param column_map The column map to be printed.
 */
std::string LayoutTuner::GetColumnMapInfo(const column_map_type& column_map) {
  std::stringstream ss;
  std::map<oid_t, std::vector<oid_t>> tile_column_map;

  // Construct a tile_id => [col_ids] map
  for (auto itr = column_map.begin(); itr != column_map.end(); itr++) {
    oid_t col_id = itr->first;
    oid_t tile_id = itr->second.first;

    if (tile_column_map.find(tile_id) == tile_column_map.end()) {
      tile_column_map[tile_id] = {};
    }

    tile_column_map[tile_id].push_back(col_id);
  }

  // Construct a string from tile_col_map
  for (auto itr = tile_column_map.begin(); itr != tile_column_map.end();
       itr++) {
    oid_t tile_id = itr->first;
    ss << tile_id << ": ";
    for (oid_t col_id : itr->second) {
      ss << col_id << " ";
    }
    ss << " :: ";
  }

  return ss.str();
}

Sample GetClustererSample(const Sample& sample, oid_t column_count) {

  // Copy over the sample
  Sample clusterer_sample = sample;

  // Figure out columns accessed, and construct a bitmap
  auto& columns_accessed = sample.GetColumnsAccessed();
  std::vector<double> columns_accessed_bitmap;

  for(oid_t column_itr = 0;
      column_itr < column_count;
      column_itr++){

    // Append column into sample
    auto column_found = std::find(columns_accessed.begin(),
                                  columns_accessed.end(),
                                  column_itr) != columns_accessed.end();

    if(column_found == true) {
      columns_accessed_bitmap.push_back(1);
    } else {
      columns_accessed_bitmap.push_back(0);
    }
  }

  PELOTON_ASSERT(columns_accessed_bitmap.size() == column_count);
  clusterer_sample.SetColumnsAccessed(columns_accessed_bitmap);

  return clusterer_sample;
}

void LayoutTuner::UpdateDefaultPartition(storage::DataTable* table) {
  oid_t column_count = table->GetSchema()->GetColumnCount();

  // Set up clusterer
  tuning::Clusterer clusterer(cluster_count, column_count, new_sample_weight);

  // Process all samples in table
  auto samples = table->GetLayoutSamples();

  // Check if we have any samples
  if (samples.empty()) {
    return;
  }

  for (auto sample : samples) {
    if (sample.GetColumnsAccessed().size() == 0) {
      continue;
    }

    // Transform the regular sample to a bitmap sample for clusterer
    // {0, 3} => { 1, 0, 0, 1}
    auto clusterer_sample = GetClustererSample(sample, column_count);

    clusterer.ProcessSample(clusterer_sample);
  }

  // Clear all samples in table
  table->ClearLayoutSamples();

  // Desired number of tiles
  auto layout = clusterer.GetPartitioning(tile_count);

  LOG_TRACE("%s", GetColumnMapInfo(layout).c_str());

  // Update table layout
  table->SetDefaultLayout(layout);
}

void LayoutTuner::Tune() {
  Timer<std::milli> timer;
  // Continue till signal is not false
  while (layout_tuning_stop == false) {
    // Go over all tables
    for (auto table : tables) {
      // Transform
      auto tile_group_count = table->GetTileGroupCount();
      auto tile_group_offset = rand() % tile_group_count;

      LOG_TRACE("Transforming tile group at offset: %lu", tile_group_offset);
      table->TransformTileGroup(tile_group_offset, theta);

      // Update partitioning periodically
      UpdateDefaultPartition(table);

      // Sleep a bit
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
    }
  }
}

void LayoutTuner::Stop() {
  // Stop tuning
  layout_tuning_stop = true;

  // Stop thread
  layout_tuner_thread.join();

  LOG_INFO("Stopped layout tuner");
}

void LayoutTuner::AddTable(storage::DataTable* table) {
  {
    std::lock_guard<std::mutex> lock(layout_tuner_mutex);
    LOG_TRACE("Layout tuner adding table : %p", table);

    tables.push_back(table);
  }
}

void LayoutTuner::ClearTables() {
  {
    std::lock_guard<std::mutex> lock(layout_tuner_mutex);
    tables.clear();
  }
}

}  // namespace indextuner
}  // namespace peloton

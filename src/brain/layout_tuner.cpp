//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_tuner.cpp
//
// Identification: src/brain/layout_tuner.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/layout_tuner.h"

#include "catalog/schema.h"
#include "common/logger.h"
#include "common/timer.h"
#include "storage/data_table.h"

namespace peloton {
namespace brain {

LayoutTuner& LayoutTuner::GetInstance() {
  static LayoutTuner layout_tuner;
  return layout_tuner;
}

LayoutTuner::LayoutTuner() {
  // Nothing to do here !
  tile_groups_transformed_ = 0;
}

LayoutTuner::~LayoutTuner() {
  // Print time breakdowns
  double transform_tg_mean, update_default_partition_mean;
  double transform_tg_sum, update_default_partition_sum;

  CalculateStatistics(transform_tg_times_, transform_tg_mean, transform_tg_sum);
  CalculateStatistics(update_default_partition_times_, update_default_partition_mean, update_default_partition_sum);

  LOG_INFO("\t[LAYOUT]\tTotal transform tilegroup time\t%lf ms", transform_tg_sum);
  LOG_INFO("\t[LAYOUT]\tTotal tile group transformed\t%d", (int)tile_groups_transformed_);
  LOG_INFO("\t[LAYOUT]\tAverage transform tilegroup time\t%lf ms", transform_tg_sum / tile_groups_transformed_);
  LOG_INFO("\t[LAYOUT]\tTotal update partition time\t%lf ms", update_default_partition_sum);
}

void LayoutTuner::Start() {
  // Set signal
  layout_tuning_stop = false;

  // Launch thread
  layout_tuner_thread = std::thread(&brain::LayoutTuner::Tune, this);
}

void LayoutTuner::CalculateStatistics(const std::vector<double> data, double &mean, double &sum) {
  if (data.size() == 0) {
    mean = 0.0;
    sum = 0.0;
  }

  sum = 0.0;

  for (auto stat : data) {
    sum += stat;
  }

  mean = sum / data.size();
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

void LayoutTuner::UpdateDefaultPartition(storage::DataTable* table) {
  oid_t column_count = table->GetSchema()->GetColumnCount();

  // Set up clusterer
  brain::Clusterer clusterer(cluster_count, column_count, new_sample_weight);

  // Process all samples in table
  auto& samples = table->GetLayoutSamples();

  // Check if we have any samples
  if (samples.empty()) {
    return;
  }

  for (auto sample : samples) {
    clusterer.ProcessSample(sample);
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

      timer.Start();
      if (table->TransformTileGroup(tile_group_offset, theta) != nullptr) {
        tile_groups_transformed_ += 1;
      }
      timer.Stop();
      transform_tg_times_.push_back(timer.GetDuration());
      timer.Reset();

      // Update partitioning periodically
      timer.Reset();
      timer.Start();
      UpdateDefaultPartition(table);
      timer.Stop();
      update_default_partition_times_.push_back(timer.GetDuration());

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

}  // End brain namespace
}  // End peloton namespace

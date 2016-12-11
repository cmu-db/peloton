//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_tuner.h
//
// Identification: src/include/brain/layout_tuner.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "brain/clusterer.h"
#include "common/types.h"
#include "common/timer.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace brain {

//===--------------------------------------------------------------------===//
// Layout Tuner
//===--------------------------------------------------------------------===//

class LayoutTuner {
 public:
  LayoutTuner(const LayoutTuner &) = delete;
  LayoutTuner &operator=(const LayoutTuner &) = delete;
  LayoutTuner(LayoutTuner &&) = delete;
  LayoutTuner &operator=(LayoutTuner &&) = delete;

  LayoutTuner();

  ~LayoutTuner();

  // Singleton
  static LayoutTuner &GetInstance();

  // Start tuning
  void Start();

  // Tune layout
  void Tune();

  // Stop tuning
  void Stop();

  // Add table to list of tables whose layout must be tuned
  void AddTable(storage::DataTable *table);

  // Clear list
  void ClearTables();

 protected:
  // Update layout of table
  void UpdateDefaultPartition(storage::DataTable *table);

  std::string GetColumnMapInfo(const column_map_type &column_map);

  void CalculateStatistics(const std::vector<double> data, double &mean, double &sum);

 private:
  // Tables whose layout must be tuned
  std::vector<storage::DataTable *> tables;

  std::mutex layout_tuner_mutex;

  // Stop signal
  std::atomic<bool> layout_tuning_stop;

  // Tuner thread
  std::thread layout_tuner_thread;

  //===--------------------------------------------------------------------===//
  // Tuner Parameters
  //===--------------------------------------------------------------------===//

  // Layout similarity threshold
  // NOTE:
  // This could be very sensitive, the measurement of schema difference in
  // datatable is divided by column_count, so could be very small. Theta should
  // also not be set to zero, otherwise it will always trigger
  // DataTable::TransformTileGroup, even if the schema is the same.
  double theta = 0.0001;

  // Sleeping period (in us)
  oid_t sleep_duration = 100;

  // Cluster count
  oid_t cluster_count = 4;

  // New sample weight
  double new_sample_weight = 0.01;

  // Desired layout tile count
  // FIXME: for join query, only two tiles in the tile group is not enough,
  // should be 3 = 2 for projected columns from two tables + 1 (other columns)
  oid_t tile_count = 2;

  // Profile times
  std::vector<double> update_default_partition_times_;
  std::vector<double> transform_tg_times_;
  oid_t tile_groups_transformed_;
};

}  // End brain namespace
}  // End peloton namespace

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
#include "common/internal_types.h"
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

  std::string GetColumnMapInfo(const column_map_type &column_map);

 protected:
  // Update layout of table
  void UpdateDefaultPartition(storage::DataTable *table);

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
  // This is a critical parameter. It measures the difference between the schema
  // of a existing tilegroup and the desired schema, and normalizes this difference
  // with respect to the column count, so that it falls within [0, 1]
  // Theta should not be set to zero, otherwise it will always trigger
  // DataTable::TransformTileGroup, even if the schema is the same.
  double theta = 0.0001;

  // Sleeping period (in us)
  oid_t sleep_duration = 100;

  // Cluster count
  oid_t cluster_count = 4;

  // New sample weight
  double new_sample_weight = 0.01;

  // Desired layout tile count
  oid_t tile_count = 2;

};

}  // namespace brain
}  // namespace peloton

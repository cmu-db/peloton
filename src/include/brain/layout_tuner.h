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

#include <vector>
#include <mutex>
#include <atomic>
#include <thread>

#include "common/types.h"

namespace peloton {

namespace storage{
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
  void AddTable(storage::DataTable* table);

  // Update layout of table
  void UpdateDefaultPartition(storage::DataTable* table);

  // Clear list
  void ClearTables();

 private:

  // Tables whose layout must be tuned
  std::vector<storage::DataTable*> tables;

  std::mutex layout_tuner_mutex;

  // Stop signal
  std::atomic<bool> layout_tuning_stop;

  // Tuner thread
  std::thread layout_tuner_thread;

  //===--------------------------------------------------------------------===//
  // Tuner Parameters
  //===--------------------------------------------------------------------===//

  // Layout similarity threshold
  double theta = 0.0;

  // Sleeping period (in us)
  oid_t sleep_duration = 100;

  // Cluster count
  oid_t cluster_count = 4;

  // New sample weight
  double new_sample_weight = 0.01;

  // Desired layout tile count
  oid_t tile_count = 2;

};


}  // End brain namespace
}  // End peloton namespace

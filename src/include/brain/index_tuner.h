//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tuner.h
//
// Identification: src/include/brain/index_tuner.h
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

namespace index{
class Index;
}

namespace storage{
class DataTable;
}

namespace brain {

class Sample;

//===--------------------------------------------------------------------===//
// Index Tuner
//===--------------------------------------------------------------------===//

class IndexTuner {

 public:

  IndexTuner(const IndexTuner &) = delete;
  IndexTuner &operator=(const IndexTuner &) = delete;
  IndexTuner(IndexTuner &&) = delete;
  IndexTuner &operator=(IndexTuner &&) = delete;

  IndexTuner();

  ~IndexTuner();

  // Singleton
  static IndexTuner &GetInstance();

  // Start tuning
  void Start();

  // Tune layout
  void Tune();

  // Stop tuning
  void Stop();

  // Add table to list of tables whose layout must be tuned
  void AddTable(storage::DataTable* table);

  // Clear list
  void ClearTables();

 protected:

  // Index building helper
  void BuildIndex(index::Index *index, storage::DataTable *table);

  // Index tuning helper
  void IndexTuneHelper(storage::DataTable* table);

  // Analyze
  void Analyze(storage::DataTable* table,
               const std::vector<brain::Sample>& samples);

 private:

  // Tables whose indices must be tuned
  std::vector<storage::DataTable*> tables;

  std::mutex index_tuner_mutex;

  // Stop signal
  std::atomic<bool> index_tuning_stop;

  // Tuner thread
  std::thread index_tuner_thread;

  //===--------------------------------------------------------------------===//
  // Tuner Parameters
  //===--------------------------------------------------------------------===//

  // Sleeping period (in us)
  oid_t sleep_duration = 10;

  // Threshold sample count
  oid_t sample_count_threshold = 300;

};


}  // End brain namespace
}  // End peloton namespace

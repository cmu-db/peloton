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

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "common/types.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
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
  void AddTable(storage::DataTable *table);

  // Add indexes to table
  void AddIndexes(storage::DataTable* table,
                  const std::vector<std::vector<double>>& suggested_indices);

  // Clear list
  void ClearTables();

  void SetDurationBetweenPauses(const oid_t& duration_between_pauses_){
    duration_between_pauses = duration_between_pauses_;
  }

  void SetDurationOfPause(const oid_t& duration_of_pause_){
    duration_of_pause = duration_of_pause_;
  }

  void SetAnalyzeSampleCountThreshold(const oid_t &analyze_sample_count_threshold_) {
    analyze_sample_count_threshold = analyze_sample_count_threshold_;
  }

  void SetTileGroupsIndexedPerIteration(const oid_t &tile_groups_indexed_per_iteration_) {
    tile_groups_indexed_per_iteration = tile_groups_indexed_per_iteration_;
  }

  void SetIndexUtilityThreshold(const double& index_utility_threshold_){
    index_utility_threshold = index_utility_threshold_;
  }

  void SetIndexCountThreshold(const oid_t& index_count_threshold_){
    index_count_threshold = index_count_threshold_;
  }

  void SetWriteRatioThreshold(const double& write_ratio_threshold_){
    write_ratio_threshold = write_ratio_threshold_;
  }

  // Get # of indexes in managed tables
  oid_t GetIndexCount() const;

 protected:
  // Index tuning helper
  void IndexTuneHelper(storage::DataTable *table);

  void BuildIndex(storage::DataTable *table,
                  std::shared_ptr<index::Index> index);

  void BuildIndices(storage::DataTable *table);

  void Analyze(storage::DataTable *table);

  double ComputeWorkloadWriteRatio(const std::vector<brain::Sample> &samples);

  void DropIndexes(storage::DataTable *table);

  void CalculateStatistics(const std::vector<double> data, double &mean, double &sum);

 private:
  // Tables whose indices must be tuned
  std::vector<storage::DataTable *> tables;

  std::mutex index_tuner_mutex;

  // Stop signal
  std::atomic<bool> index_tuning_stop;

  // Tuner thread
  std::thread index_tuner_thread;

  //===--------------------------------------------------------------------===//
  // Tuner Parameters
  //===--------------------------------------------------------------------===//

  // Threshold sample count

  // duration between pauses
  oid_t duration_between_pauses = 1000;

  // duration of pause (in ms)
  oid_t duration_of_pause = 100;

  // frequency with which index analysis happens
  oid_t analyze_sample_count_threshold = 10;

  // # of tile groups to be indexed per iteration
  oid_t tile_groups_indexed_per_iteration = 10;

  // alpha (weight for old samples)
  double alpha = 0.2;

  // average write ratio
  double average_write_ratio = INVALID_RATIO;

  //===--------------------------------------------------------------------===//
  // DROP Thresholds
  //===--------------------------------------------------------------------===//

  // index utility threshold below which it will be dropped
  double index_utility_threshold = 0.25;

  // maximum # of indexes per table
  oid_t index_count_threshold = 10;

  // write intensive workload ratio threshold
  double write_ratio_threshold = 0.75;

  // Profile time
  std::vector<double> build_indices_times_;
  std::vector<double> analyze_times_;
  std::vector<double> add_indexes_times_;
  std::vector<double> update_index_util_times_;
  std::vector<double> compute_rw_ratio_times_;
  std::vector<double> get_suggested_indexes_times_;
  std::vector<double> get_frequent_samples_times_;

  oid_t tile_groups_indexed_;
};

}  // End brain namespace
}  // End peloton namespace

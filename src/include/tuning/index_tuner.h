//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// indextuner.h
//
// Identification: src/include/tuning/indextuner.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "common/internal_types.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class DataTable;
}

namespace tuning {

/**
 * Load statistics for Index Tuner from a file
 *
 * @param[in]  path  The path
 */
void LoadStatsFromFile(const std::string& path);


/**
 * @brief      Class for sample.
 */
class Sample;

//===--------------------------------------------------------------------===//
// Index Tuner
//===--------------------------------------------------------------------===//

/**
 * @brief      Class for index tuner.
 */
class IndexTuner {
 public:
  IndexTuner(const IndexTuner &) = delete;
  IndexTuner &operator=(const IndexTuner &) = delete;
  IndexTuner(IndexTuner &&) = delete;
  IndexTuner &operator=(IndexTuner &&) = delete;

  IndexTuner();

  ~IndexTuner();

  /**
   * Singleton
   *
   * @return     The instance.
   */
  static IndexTuner &GetInstance();

  /**
   * Start tuning
   */
  void Start();

  /**
   * Tune layout
   */
  void Tune();

  /**
   * Stop tuning
   */
  void Stop();

  /**
   * Add table to list of tables whose layout must be tuned
   *
   * @param      table  The table
   */
  void AddTable(storage::DataTable *table);

  /**
   * Clear list
   */
  void ClearTables();

  /**
   * @brief      Sets the duration between pauses.
   *
   * @param[in]  duration_between_pauses_  The duration between pauses
   */
  void SetDurationBetweenPauses(const oid_t &duration_between_pauses_) {
    duration_between_pauses = duration_between_pauses_;
  }

  /**
   * @brief      Sets the duration of pause.
   *
   * @param[in]  duration_of_pause_  The duration of pause
   */
  void SetDurationOfPause(const oid_t &duration_of_pause_) {
    duration_of_pause = duration_of_pause_;
  }

  /**
   * @brief      Sets the analyze sample count threshold.
   *
   * @param[in]  analyze_sample_count_threshold_  The analyze sample count threshold
   */
  void SetAnalyzeSampleCountThreshold(
      const oid_t &analyze_sample_count_threshold_) {
    analyze_sample_count_threshold = analyze_sample_count_threshold_;
  }

  /**
   * @brief      Sets the tile groups indexed per iteration.
   *
   * @param[in]  tile_groups_indexed_per_iteration_  The tile groups indexed per iteration
   */
  void SetTileGroupsIndexedPerIteration(
      const oid_t &tile_groups_indexed_per_iteration_) {
    tile_groups_indexed_per_iteration = tile_groups_indexed_per_iteration_;
  }

  /**
   * @brief      Sets the index utility threshold.
   *
   * @param[in]  index_utility_threshold_  The index utility threshold
   */
  void SetIndexUtilityThreshold(const double &index_utility_threshold_) {
    index_utility_threshold = index_utility_threshold_;
  }

  /**
   * @brief      Sets the index count threshold.
   *
   * @param[in]  index_count_threshold_  The index count threshold
   */
  void SetIndexCountThreshold(const oid_t &index_count_threshold_) {
    index_count_threshold = index_count_threshold_;
  }

  /**
   * @brief      Sets the write ratio threshold.
   *
   * @param[in]  write_ratio_threshold_  The write ratio threshold
   */
  void SetWriteRatioThreshold(const double &write_ratio_threshold_) {
    write_ratio_threshold = write_ratio_threshold_;
  }

  /**
   * Get # of indexes in managed tables
   *
   * @return     The index count.
   */
  oid_t GetIndexCount() const;

  /**
   * Bootstrap for TPCC
   *
   * @param[in]  path  The path
   */
  void BootstrapTPCC(const std::string& path);

  /**
   * @brief      Sets the visibility mode.
   */
  void SetVisibilityMode() {
    visibility_mode_ = true;
  }

 protected:

  /**
   * Add indexes to table
   *
   * @param      table              The table
   * @param[in]  suggested_indices  The suggested indices
   */
  void AddIndexes(storage::DataTable *table,
                  const std::vector<std::vector<double>> &suggested_indices);

  // Index tuning helper
  //
  // @param      table  The table
  //
  void IndexTuneHelper(storage::DataTable *table);

  /**
   * @brief      Builds an index.
   *
   * @param      table  The table
   * @param[in]  index  The index
   */
  void BuildIndex(storage::DataTable *table,
                  std::shared_ptr<index::Index> index);

  /**
   * @brief      Builds indices.
   *
   * @param      table  The table
   */
  void BuildIndices(storage::DataTable *table);

  void Analyze(storage::DataTable *table);

  /**
   * @brief      Calculates the workload write ratio.
   *
   * @param[in]  samples  The samples
   *
   * @return     The workload write ratio.
   */
  double ComputeWorkloadWriteRatio(const std::vector<tuning::Sample> &samples);

  void DropIndexes(storage::DataTable *table);

  /**
   * @brief      Calculates the statistics.
   *
   * @param[in]  data  The data
   * @param      mean  The mean
   * @param      sum   The sum
   */
  void CalculateStatistics(const std::vector<double> data, double &mean,
                           double &sum);

 private:
  /** Tables whose indices must be tuned */
  std::vector<storage::DataTable *> tables;

  std::mutex index_tuner_mutex;

  /** Stop signal */
  std::atomic<bool> index_tuning_stop;

  /** Tuner thread */
  std::thread index_tuner_thread;

  //===--------------------------------------------------------------------===//
  // Tuner Parameters
  //===--------------------------------------------------------------------===//

  // Threshold sample count

  /** duration between pauses (in ms) */
  oid_t duration_between_pauses = 1000;

  /** duration of pause (in ms) */
  oid_t duration_of_pause = 1000;

  /** frequency with which index analysis happens */
  oid_t analyze_sample_count_threshold = 1;

  /** # of tile groups to be indexed per iteration */
  oid_t tile_groups_indexed_per_iteration = 10;

  /** alpha (weight for old samples) */
  double alpha = 0.2;

  /** average write ratio */
  double average_write_ratio = INVALID_RATIO;

  //===--------------------------------------------------------------------===//
  // DROP Thresholds
  //===--------------------------------------------------------------------===//

  /** index utility threshold below which it will be dropped */
  double index_utility_threshold = 0.25;

  /** maximum # of indexes per table */
  oid_t index_count_threshold = 10;

  /** write intensive workload ratio threshold */
  double write_ratio_threshold = 0.75;

  oid_t tile_groups_indexed_;

  /** visibility mode */
  bool visibility_mode_ = false;
};

}  // namespace indextuner
}  // namespace peloton

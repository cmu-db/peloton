//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lspi_tuner.h
//
// Identification: src/include/brain/indextune/lspi/lspi_tuner.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include "brain/indextune/compressed_index_config.h"
#include "brain/indextune/compressed_index_config_util.h"
#include "brain/indextune/lspi/lstd.h"
#include "brain/indextune/lspi/rlse.h"
#include "brain/util/eigen_util.h"

/**
 * Least-Squares Policy Iteration based Index tuning
 * (Derived from Cost Model Oblivious DB Tuning by Basu et. al.)
 * This can be extended to any configuration knob tuning problem.
 * For now, we assume one instance of the tuner per database.
 */
namespace peloton {
namespace brain {
class LSPIIndexTuner {
 public:
  explicit LSPIIndexTuner(
      const std::string &db_name, catalog::Catalog *cat = nullptr,
      concurrency::TransactionManager *txn_manager = nullptr);
  /**
   * Given a recent set of queries and their latency on the current
   * configuration
   * this function will automatically tune the database for future workloads.
   * Currently it only supports IndexTuning but should be relatively simple to
   * support
   * more utility functions.
   * @param query_latency_pairs: vector of <query string, latency> pairs
   */
  void Tune(const std::vector<std::string> &queries,
            const std::vector<double> &query_latencies);
  void FindOptimalConfig(double max_cost,
                         const boost::dynamic_bitset<> &curr_config_set,
                         const boost::dynamic_bitset<> &add_candidate_set,
                         const boost::dynamic_bitset<> &drop_candidate_set,
                         boost::dynamic_bitset<> &optimal_config_set);

 private:
  // Database to tune
  std::string db_name_;
  // Index configuration object - Represents current set of indexes compactly
  // and exposes APIs for generating a search space for our RL algorithm
  std::unique_ptr<CompressedIndexConfigContainer> index_config_;
  // RLSE model for computing immediate cost of an action
  std::unique_ptr<RLSEModel> rlse_model_;
  // LSTD model for computing
  std::unique_ptr<LSTDModel> lstd_model_;
  // Previous config feature vector
  vector_eig prev_config_vec;
};
}  // namespace brain
}  // namespace peloton
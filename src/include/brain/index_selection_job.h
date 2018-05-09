//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_job.h
//
// Identification: src/include/brain/index_selection_job.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "brain.h"
#include "brain/index_selection_util.h"

namespace peloton {

namespace brain {
class IndexSelectionJob : public BrainJob {
 public:
  IndexSelectionJob(BrainEnvironment *env, uint64_t num_queries_threshold)
      : BrainJob(env),
        last_timestamp_(0),
        num_queries_threshold_(num_queries_threshold) {}
  /**
   * Task function.
   * @param env
   */
  void OnJobInvocation(BrainEnvironment *env);

 private:
  /**
   * Go through the queries and return the timestamp of the latest query.
   * @return latest timestamp
   */
  static uint64_t GetLatestQueryTimestamp(
      std::vector<std::pair<uint64_t, std::string>> *);
  /**
   * Sends an RPC message to server for creating indexes.
   * @param table_name
   * @param keys
   */
  void CreateIndexRPC(brain::HypotheticalIndexObject *index);

  /**
   * Sends an RPC message to server for drop indexes.
   * @param index
   */
  void DropIndexRPC(oid_t database_oid, catalog::IndexCatalogObject *index);

  /**
   * Timestamp of the latest query of the recently processed
   * query workload.
   */
  uint64_t last_timestamp_;
  /**
   * Tuning threshold in terms of queries
   * Run the index suggestion only if the number of new queries
   * in the workload exceeds this number
   */
  uint64_t num_queries_threshold_;
};
}  // peloton brain

}  // namespace peloton

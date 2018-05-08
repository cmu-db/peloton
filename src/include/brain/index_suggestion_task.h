//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_suggestion_task.h
//
// Identification: src/include/brain/index_suggestion_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "brain.h"
#include "brain/index_selection_util.h"

namespace peloton {

namespace brain {
class IndexSuggestionTask {
 public:
  /**
   * Task function.
   * @param env
   */
  static void Task(BrainEnvironment *env);

  /**
   * Sends an RPC message to server for creating indexes.
   * @param table_name
   * @param keys
   */
  static void CreateIndexRPC(brain::HypotheticalIndexObject *index);

  /**
   * Task interval
   */
  static struct timeval interval;

  /**
   * Timestamp of the latest query of the recently processed
   * query workload.
   */
  static uint64_t last_timestamp;

  /**
   * Tuning threshold in terms of queries
   * Run the index suggestion only if the number of new queries
   * in the workload exceeds this number
   */
  static uint64_t tuning_threshold;

  /**
   *
   */
  static size_t max_index_cols;

  /**
   *
   */
  static size_t enumeration_threshold;

  /**
   *
   */
  static size_t num_indexes;

 private:
  /**
   * Go through the queries and return the timestamp of the latest query.
   * @return latest timestamp
   */
  static uint64_t GetLatestQueryTimestamp(
      std::vector<std::pair<uint64_t, std::string>>*);
};
}  // peloton brain

}  // namespace peloton

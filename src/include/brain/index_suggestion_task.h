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

namespace peloton {

namespace brain {
class IndexSuggestionTask {
 public:
  static void Task(BrainEnvironment *env);
  static void SendIndexCreateRPCToServer(std::string table_name,
                                         std::vector<oid_t> keys);
  static struct timeval interval;
  static uint64_t last_timestamp;
  static uint64_t tuning_threshold;
};
}  // peloton brain

}  // namespace peloton

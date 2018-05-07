//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_suggestion_task.cpp
//
// Identification: src/brain/index_suggestion_task.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/brain/index_suggestion_task.h"

namespace peloton {

namespace brain {

// Interval in seconds.
struct timeval IndexSuggestionTask::interval{10, 0};

void IndexSuggestionTask::Task(BrainEnvironment *env) {
  (void) env;
  LOG_INFO("Started Index Suggestion Task");
}

}

}

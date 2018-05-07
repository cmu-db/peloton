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
    static struct timeval interval;
  };
} // peloton brain

} // namespace peloton

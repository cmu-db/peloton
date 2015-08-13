//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nested_loop_join_executor.h
//
// Identification: src/backend/executor/nested_loop_join_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/abstract_join_executor.h"

#include <vector>

namespace peloton {
namespace executor {

class NestedLoopJoinExecutor : public AbstractJoinExecutor {
  NestedLoopJoinExecutor(const NestedLoopJoinExecutor &) = delete;
  NestedLoopJoinExecutor &operator=(const NestedLoopJoinExecutor &) = delete;

 public:
  explicit NestedLoopJoinExecutor(planner::AbstractPlan *node,
                                  ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

  /** @brief Starting left table scan. */
  bool left_scan_start = false;

 private:
  // nothing special here
};

}  // namespace executor
}  // namespace peloton

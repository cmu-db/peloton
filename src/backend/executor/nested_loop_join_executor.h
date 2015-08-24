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

 private:

  /* Buffer to store right child's result tiles */
  std::vector<executor::LogicalTile*> right_result_tiles_;
  bool right_child_done_ = false;
  size_t right_result_itr_ = 0;

  std::unique_ptr<LogicalTile> cur_left_tile_;

};

}  // namespace executor
}  // namespace peloton

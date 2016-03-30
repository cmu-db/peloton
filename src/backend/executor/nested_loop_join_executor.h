//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_executor.h
//
// Identification: src/backend/executor/nested_loop_join_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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
  explicit NestedLoopJoinExecutor(const planner::AbstractPlan *node,
                                  ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  // Right child's result tiles iterator
  size_t right_result_itr_ = 0;

  size_t left_result_itr_ = 0;  // added by Michael
};

}  // namespace executor
}  // namespace peloton

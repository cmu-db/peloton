//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_executor.h
//
// Identification: src/include/executor/nested_loop_join_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_join_executor.h"

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
  bool Old_DExecute();
  bool DExecute();

 private:
  // Right child's result tiles iterator
  size_t right_result_itr_ = 0;

  size_t left_result_itr_ = 0;  // added by Michael

  // columns in left table for join predicate.
  std::vector<oid_t> join_column_ids_;
};

}  // namespace executor
}  // namespace peloton

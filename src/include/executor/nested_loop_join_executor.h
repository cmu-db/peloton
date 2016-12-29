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
  bool DExecute();

 private:
  // Right child's result tiles iterator
  size_t right_result_itr_ = 0;

  // For left result, we only use the current left tile, which is cached in left
  // result vector. Although, we only need to cache one tile (current tile),
  // there already exists left result vector, I just re-use it.
  size_t left_result_itr_ = 0;  // added by Michael

  // The offset in the current tile. Each time we get a left tile, and iterate
  // this left tile and pick each row to lookup the right table. Since each time
  // we return a tile result, that is one row in the left tile, and a right
  // tile, we need to keep a pointer to know which row should start next time.
  size_t left_tile_row_itr = 0;

  std::unique_ptr<LogicalTile> output_tile;
};

}  // namespace executor
}  // namespace peloton

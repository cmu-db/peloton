//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// nested_loop_join_executor.h
//
// Identification: src/include/executor/nested_loop_join_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_join_executor.h"

namespace peloton {
namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
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

  // The offset in the current tile. Each time we get a left tile, and iterate
  // this left tile and pick each row to lookup the right table. Since each time
  // we return a tile result, that is one row in the left tile, and a right
  // tile, we need to keep a pointer to know which row should start next time.
  size_t left_tile_row_itr_ = 0;

  // This is used the cache the left result to save memory. It is not necessary
  // to cache all results in a vector, so I only use this pointer to cache the
  // left result. Each time, we should first finish left and then call child[0]
  // to get the next one left tile
  std::unique_ptr<LogicalTile> left_tile_;

  // Since we cache the left tile, when iterate each row in left tile, we will
  // return the combine result when there is a matched right tile. So next time,
  // we will begin from the point of last time, if left_tile_done is false
  bool left_tile_done_ = true;
};

}  // namespace executor
}  // namespace peloton

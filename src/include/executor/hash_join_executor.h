//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_executor.h
//
// Identification: src/include/executor/hash_join_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "executor/abstract_join_executor.h"
#include "planner/hash_join_plan.h"
#include "executor/hash_executor.h"

namespace peloton {
namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class HashJoinExecutor : public AbstractJoinExecutor {
  HashJoinExecutor(const HashJoinExecutor &) = delete;
  HashJoinExecutor &operator=(const HashJoinExecutor &) = delete;

 public:
  explicit HashJoinExecutor(const planner::AbstractPlan *node,
                            ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  HashExecutor *hash_executor_ = nullptr;

  bool hashed_ = false;

  std::deque<LogicalTile *> buffered_output_tiles;
  std::vector<std::unique_ptr<LogicalTile>> right_tiles_;

  // logical tile iterators
  size_t left_logical_tile_itr_ = 0;
  size_t right_logical_tile_itr_ = 0;
};

}  // namespace executor
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_executor.h
//
// Identification: src/backend/executor/hash_join_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <vector>

#include "backend/executor/abstract_join_executor.h"
#include "backend/planner/hash_join_plan.h"
#include "backend/executor/hash_executor.h"

namespace peloton {
namespace executor {

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

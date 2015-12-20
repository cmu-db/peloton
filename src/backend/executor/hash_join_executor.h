/*-------------------------------------------------------------------------
 *
 * hash_join.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/hash_join_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

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
};

}  // namespace executor
}  // namespace peloton

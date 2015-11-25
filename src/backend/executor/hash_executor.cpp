//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_set_op_executor.cpp
//
// Identification: src/backend/executor/hash_set_op_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "backend/common/logger.h"
#include "backend/common/value.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/hash_executor.h"
#include "backend/planner/hash_plan.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
HashExecutor::HashExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool HashExecutor::DInit() {
  assert(children_.size() == 2);

  return true;
}

bool HashExecutor::DExecute() {
  return false;
}

} /* namespace executor */
} /* namespace peloton */

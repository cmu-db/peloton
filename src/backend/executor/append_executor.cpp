//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// append_executor.cpp
//
// Identification: src/backend/executor/append_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/append_executor.h"

#include "backend/planner/append_plan.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
AppendExecutor::AppendExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Basic checks.
 * @return true on success, false otherwise.
 */
bool AppendExecutor::DInit() {
  // should have >= 2 children, otherwise pointless.
  PL_ASSERT(children_.size() >= 2);
  PL_ASSERT(cur_child_id_ == 0);

  return true;
}

bool AppendExecutor::DExecute() {
  LOG_TRACE("Append executor ");

  while (cur_child_id_ < children_.size()) {
    if (children_[cur_child_id_]->Execute()) {
      SetOutput(children_[cur_child_id_]->GetOutput());
      return true;
    } else
      cur_child_id_++;
  }

  return false;
}

} /* namespace executor */
} /* namespace peloton */

//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// limit_executor.cpp
//
// Identification: src/backend/executor/limit_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/limit_executor.h"

#include "../planner/limit_plan.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 * @param node  LimitNode plan node corresponding to this executor
 */
LimitExecutor::LimitExecutor(planner::AbstractPlan *node,
                             ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool LimitExecutor::DInit() {
  assert(children_.size() == 1);

  num_skipped_ = 0;
  num_returned_ = 0;

  return true;
}

/**
 * @brief Creates logical tiles from the input logical tiles after applying
 * limit.
 * @return true on success, false otherwise.
 */
bool LimitExecutor::DExecute() {
  // Grab data from plan node
  const planner::LimitPlan &node = GetPlanNode<planner::LimitPlan>();
  const size_t limit = node.GetLimit();
  const size_t offset = node.GetOffset();

  LOG_TRACE("Limit executor \n");

  while (num_returned_ < limit && children_[0]->Execute()) {
    std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

    for (oid_t tuple_id : *tile) {
      // "below" tuples
      if (num_skipped_ < offset) {
        tile->RemoveVisibility(tuple_id);
        num_skipped_++;
      }
      // good tuples
      else if (num_returned_ < limit) {
        num_returned_++;
      }
      // "above" tuples
      else {
        tile->RemoveVisibility(tuple_id);
      }
    }

    // Avoid returning empty tiles
    if (tile->GetTupleCount() > 0) {
      SetOutput(tile.release());
      return true;
    }
  }

  return false;
}

} /* namespace executor */
} /* namespace peloton */

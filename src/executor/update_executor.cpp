/*-------------------------------------------------------------------------
 *
 * update_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/update_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/update_executor.h"

#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "planner/update_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(planner::AbstractPlanNode *node,
                               Context *context)
: AbstractExecutor(node, context) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
  assert(children_.size() <= 1);
  return true;
}

/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
  assert(children_.size() == 0);
  assert(context_);

  return true;
}

} // namespace executor
} // namespace nstore

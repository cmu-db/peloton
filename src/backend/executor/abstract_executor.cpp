/**
 * @brief Base class for all executors.
 *
 * Copyright(c) 2015, CMU
 */

#include "backend/executor/abstract_executor.h"
#include "backend/common/logger.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for AbstractExecutor.
 * @param node Abstract plan node corresponding to this executor.
 */
AbstractExecutor::AbstractExecutor(planner::AbstractPlanNode *node)
: node_(node) {
}

AbstractExecutor::AbstractExecutor(planner::AbstractPlanNode *node, concurrency::Transaction *transaction)
: transaction_(transaction), node_(node) {
}

void AbstractExecutor::SetOutput(LogicalTile* table) {
  output.reset(table);
}

// Transfers ownership
LogicalTile* AbstractExecutor::GetOutput() {
  assert(output.get() != nullptr);
  return output.release();
}

/**
 * @brief Add child executor to this executor node.
 * @param child Child executor to add.
 */
void AbstractExecutor::AddChild(AbstractExecutor *child) {
  children_.push_back(child);
}

/**
 * @brief Initializes the executor.
 *
 * This function executes any initialization code common to all executors.
 * It recursively initializes all children of this executor in the execution
 * tree. It calls SubInit() which is implemented by the subclass.
 *
 * @return true on success, false otherwise.
 */
bool AbstractExecutor::Init() {
  bool status = false;

  for (auto child : children_) {
    status = child->Init();
    if(status == false) {
      LOG_ERROR("Initialization failed in child executor with plan id : %d\n", child->node_->GetPlanNodeId());
      return false;
    }
  }

  status = DInit();
  if(status == false) {
    LOG_ERROR("Initialization failed in executor with plan id : %d\n", node_->GetPlanNodeId());
    return false;
  }

  return true;
}

/**
 * @brief Returns next tile processed by this executor.
 *
 * This function is the backbone of the tile-based volcano-style execution
 * model we are using.
 *
 * @return Pointer to the logical tile processed by this executor.
 */
bool AbstractExecutor::Execute() {
  //TODO In the future, we might want to pass some kind of executor state to
  // GetNextTile. e.g. params for prepared plans.

  bool status = DExecute();

  return status;
}

} // namespace executor
} // namespace peloton

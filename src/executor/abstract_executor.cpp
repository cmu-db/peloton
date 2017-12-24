//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_executor.cpp
//
// Identification: src/executor/abstract_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/value.h"
#include "common/logger.h"
#include "executor/abstract_executor.h"
#include "executor/executor_context.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for AbstractExecutor.
 * @param node Abstract plan node corresponding to this executor.
 */
AbstractExecutor::AbstractExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
    : node_(node), executor_context_(executor_context) {}

void AbstractExecutor::SetOutput(LogicalTile *table) { output.reset(table); }

// Transfers ownership
LogicalTile *AbstractExecutor::GetOutput() { return output.release(); }

/**
 * @brief Add child executor to this executor node.
 * @param child Child executor to add.
 */
void AbstractExecutor::AddChild(AbstractExecutor *child) {
  children_.push_back(child);
}

const std::vector<AbstractExecutor *> &AbstractExecutor::GetChildren() const {
  return children_;
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
    if (status == false) {
      LOG_ERROR("Initialization failed in child executor with plan id : %s",
                child->node_->GetInfo().c_str());
      return false;
    }
  }

  status = DInit();
  if (status == false) {
    LOG_ERROR("Initialization failed in executor with plan id : %s",
              node_->GetInfo().c_str());
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
  // TODO In the future, we might want to pass some kind of executor state to
  // GetNextTile. e.g. params for prepared plans.

  bool status = DExecute();

  return status;
}

}  // namespace executor
}  // namespace peloton

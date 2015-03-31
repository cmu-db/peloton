/**
 * @brief Base class for all executors.
 *
 * TODO(3/29/15): Note that this class will probably undergo many changes as
 * we come accross implementation issues. The interface is still in flux.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"

namespace nstore {
namespace executor {

/**
 * @brief This constructor should only be called in the constructors of
 *        subclasses.
 */
AbstractExecutor::AbstractExecutor(
    std::unique_ptr<planner::AbstractPlanNode> abstract_node,
    std::vector<AbstractExecutor *>& children)
  : abstract_node_(std::move(abstract_node)),
    children_(children) {
}

/**
 * @brief Initializes the executor.
 *
 * This function executes any initialization code common to all executors.
 * It recursively initializes all children of this executor in the execution
 * tree. It calls SubInit() which is implemented by the subclass.
 *
 * @return True on success, false otherwise.
 */
bool AbstractExecutor::Init() {
  for (unsigned int i = 0; i < children_.size(); i++) {
    children_[i]->Init();
  }
  return SubInit();
}

/**
 * @brief Returns next tile processed by this executor.
 *
 * This function is the backbone of the tile-based volcano-style execution
 * model we are using.
 *
 * @return Pointer to the logical tile processed by this executor.
 */
LogicalTile *AbstractExecutor::GetNextTile() {
  //TODO In the future, we might want to pass some kind of executor state to
  // GetNextTile. e.g. params for prepared plans.

  return SubGetNextTile();
}

/**
 * @brief Releases resources used by this executor.
 * 
 * Recursively releases resources used by children executors.
 * TODO Who should delete the executors?
 * TODO Why do we need a separate function to cleanup? Won't the destructor
 * suffice?
 */
void AbstractExecutor::CleanUp() {
  SubCleanUp();
  for (unsigned int i = 0; i < children_.size(); i++) {
    children_[i]->CleanUp();
  }
}

} // namespace executor
} // namespace nstore

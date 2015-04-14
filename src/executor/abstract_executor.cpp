/**
 * @brief Base class for all executors.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for AbstractExecutor.
 * @param node Abstract plan node corresponding to this executor.
 */
AbstractExecutor::AbstractExecutor(const planner::AbstractPlanNode *node)
: node(node),
  output(nullptr) {
}

/**
 * @brief Convenience method to return plan node corresponding to this
 *        executor, appropriately type-casted.
 *
 * @return Reference to plan node.
 */
template <class T> T &AbstractExecutor::GetNode() {
  T *node = dynamic_cast<T *>(node);
  assert(node);
  return *node;
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
bool AbstractExecutor::BaseInit() {

  return Init();
}

/**
 * @brief Releases resources used by this executor.
 * 
 * Recursively releases resources used by children executors.
 * TODO Who should delete the executors?
 * TODO Why do we need a separate function to cleanup? Won't the destructor
 * suffice?
 */
bool AbstractExecutor::BaseCleanUp() {

  return CleanUp();
}

} // namespace executor
} // namespace nstore

/**
 * @brief Header file for abstract executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <cassert>
#include <memory>
#include <vector>

#include "executor/logical_tile.h"
#include "planner/abstract_plan_node.h"

namespace nstore {
namespace executor {

class AbstractExecutor {

 public:
  AbstractExecutor(const AbstractExecutor &) = delete;
  AbstractExecutor& operator=(const AbstractExecutor &) = delete;

  virtual ~AbstractExecutor(){}

  bool Init();

  bool Execute();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(AbstractExecutor *child);

  void SetOutput(LogicalTile* val);

  LogicalTile *GetOutput();

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType(){
    return PLAN_NODE_TYPE_INVALID;
  }

 protected:
  explicit AbstractExecutor(planner::AbstractPlanNode *node);

  /** @brief Init function to be overriden by subclass. */
  virtual bool SubInit(){
    return true;
  }

  /** @brief Workhorse function to be overriden by subclass. */
  virtual bool SubExecute(){
    return true;
  }

  /** @brief Children nodes of this executor in the executor tree. */
  std::vector<AbstractExecutor*> children_;

  /**
   * @brief Convenience method to return plan node corresponding to this
   *        executor, appropriately type-casted.
   *
   * @return Reference to plan node.
   */
  template <class T> inline T& GetNode() {
    T *node = dynamic_cast<T *>(node_);
    assert(node);
    return *node;
  }

 private:

  // Output logical tile
  // This is where we will write the results of the plan node's execution
  std::unique_ptr<LogicalTile> output;

  /** @brief Plan node corresponding to this executor. */
  planner::AbstractPlanNode *node_ = nullptr;
};

} // namespace executor
} // namespace nstore

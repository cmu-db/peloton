/**
 * @brief Header file for abstract executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <cassert>

#include <vector>

namespace nstore {

namespace planner {
class AbstractPlanNode;
}

namespace executor {
class LogicalTile;

class AbstractExecutor {
 public:
  bool Init();

  LogicalTile *GetNextTile();

  void AddChild(AbstractExecutor *child);

 protected:
  explicit AbstractExecutor(const planner::AbstractPlanNode *node);

  /** @brief Init function to be overriden by subclass. */
  virtual bool SubInit() = 0;

  /** @brief Workhorse function to be overriden by subclass. */
  virtual LogicalTile *SubGetNextTile() = 0;

  /** @brief Children nodes of this executor in the executor tree. */
  std::vector<AbstractExecutor *> children_;

  /**
   * @brief Convenience method to return plan node corresponding to this
   *        executor, appropriately type-casted.
   *
   * @return Reference to plan node.
   */
  template <class T> const T &GetNode() {
    const T *node = dynamic_cast<const T *>(node_);
    assert(node);
    return *node;
  }

 private:
  /** @brief Plan node corresponding to this executor. */
  const planner::AbstractPlanNode *node_;
};

} // namespace executor
} // namespace nstore

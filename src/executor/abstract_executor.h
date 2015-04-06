/**
 * @brief Header file for abstract executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

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

  void CleanUp();

 protected:
  explicit AbstractExecutor(const planner::AbstractPlanNode *node);

  template <class T> T &GetNode();

  void AddChild(AbstractExecutor *child);

  /** @brief Init function to be overriden by subclass. */
  virtual bool SubInit() = 0;

  /** @brief Workhorse function to be overriden by subclass. */
  virtual LogicalTile *SubGetNextTile() = 0;

  /** @brief Clean up function to be overriden by subclass. */
  virtual void SubCleanUp() = 0;

  /** @brief Children nodes of this executor in the executor tree. */
  std::vector<AbstractExecutor *> children_;

 private:
  /** @brief Plan node corresponding to this executor. */
  const planner::AbstractPlanNode *node_;
};

} // namespace executor
} // namespace nstore

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

  // Executors are initialized once when the catalog is loaded
  bool BaseInit();

  // Invoke a plannode's associated executor
  bool BaseExecute();

  // Clean up any stuff
  bool BaseCleanUp();

  virtual ~AbstractExecutor() {}

 protected:

  explicit AbstractExecutor(const planner::AbstractPlanNode *node);

  // Get associated plan node
  template <class T> T &GetNode();

  // Concrete executor classes implement initialization
  virtual bool Init() = 0;

  // Concrete executor classes implement execution
  virtual bool Execute() = 0;

  // clean up function to be overriden by subclass.
  virtual bool CleanUp() = 0;

 private:

  // plan node corresponding to this executor
  const planner::AbstractPlanNode *node;

  // output of execution
  LogicalTile *output;

  // execution context
  //ExecutorContext *context;

};

} // namespace executor
} // namespace nstore

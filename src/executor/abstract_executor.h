/**
 * @brief Header file for abstract executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/logical_tile.h"
#include "planner/abstract_plan_node.h"

namespace nstore {
namespace executor {

class AbstractExecutor {
 public:
 
 	bool Init();
 
 	LogicalTile *GetNextTile();
 
 	void CleanUp();
 
 	virtual ~AbstractExecutor(){
 		delete abstract_node_;
 	}
 
 protected:
 
 	AbstractExecutor(planner::AbstractPlanNode *abstract_node);

  /** @brief Init function to be overriden by subclass executors. */
  virtual bool SubInit() = 0;

  /** @brief GetNextTile function to be overriden by subclass executors. */
  virtual LogicalTile *SubGetNextTile() = 0;

  /** @brief CleanUp function to be overriden by subclass executors. */
  virtual void SubCleanUp() = 0;

 	planner::AbstractPlanNode *abstract_node_;
 
};

} // namespace executor
} // namespace nstore

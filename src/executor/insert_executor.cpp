/*-------------------------------------------------------------------------
 *
 * insert_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/insert_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/insert_executor.h"

#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "planner/insert_node.h"

namespace nstore {

namespace storage {
class Tile;
}

namespace executor {

/**
 * @brief Constructor for concat executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(planner::AbstractPlanNode *node)
: AbstractExecutor(node) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DInit() {
  assert(children_.size() <= 1);
  return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {
  assert(children_.size() <= 1);

  const planner::InsertNode &node = GetNode<planner::InsertNode>();

  // Insert given tuple into table
  if(children_.size() == 0){
    storage::Table *target_table = node.GetTable();

  }
  // Insert given logical tile into table
  else{

  }

  return true;
}

} // namespace executor
} // namespace nstore

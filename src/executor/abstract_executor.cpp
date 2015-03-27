/**
 * @brief Base class for all executors.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Abstract Executor
//===--------------------------------------------------------------------===//

AbstractExecutor::AbstractExecutor(planner::AbstractPlanNode *abstract_node)
  : abstract_node_(abstract_node) {
}

bool AbstractExecutor::Init() {
  //TODO Insert any code common to all executors here.

  return SubInit();
}

LogicalTile *AbstractExecutor::GetNextTile() {
  //TODO Insert any code common to all executors here.
  //TODO In the future, we might want to pass some kind of executor state to GetNextTile.
  // e.g. params for prepared plans.

  return SubGetNextTile();
}

void AbstractExecutor::CleanUp() {
  //TODO Insert any code common to all executors here.

  SubCleanUp();
}

} // namespace executor
} // namespace nstore

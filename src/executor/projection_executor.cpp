/**
 * @brief Executor for projection node.
 *
 * Note that this executor does not materialize any changes.
 * TODO For now, we don't allow changing the ordering of columns. That will be
 * deferred to the materialization node. The planner has to be aware of this.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/projection_executor.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Projection Executor
//===--------------------------------------------------------------------===//

bool ProjectionExecutor::SubInit() {
  //TODO
}

LogicalTile *ProjectionExecutor::SubGetNextTile() {
  //TODO
  return NULL;
}

void SubCleanUp() {
  //TODO
}

} // namespace executor
} // namespace nstore

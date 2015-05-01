/**
 * @brief Executor for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/seq_scan_executor.h"

#include <memory>

#include "executor/logical_tile.h"
#include "planner/seq_scan_node.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(planner::AbstractPlanNode *node)
  : AbstractExecutor(node) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  assert(children_.size() == 0);
  return true;
}

/**
 * @brief Creates logical tile from tile group and applys scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {
  assert(children_.size() == 0);

  // Grab data from plan node.
  //const planner::SeqScanNode &node = GetNode<planner::SeqScanNode>();
  //std::unique_ptr<LogicalTile> logical_tile;
  //TODO

  //SetOutput(logical_tile.release());
  return true;
}

} // namespace executor
} // namespace nstore

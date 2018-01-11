//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_scan_executor.cpp
//
// Identification: src/executor/abstract_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/abstract_scan_executor.h"

#include "common/internal_types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "common/container_tuple.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"

#include "common/logger.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 * @param node AbstractScanNode node corresponding to this executor.
 */
AbstractScanExecutor::AbstractScanExecutor(const planner::AbstractPlan *node,
                                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Extract predicate and simple projections
 * @return true on success, false otherwise.
 */
bool AbstractScanExecutor::DInit() {
  PL_ASSERT(children_.size() == 0 || children_.size() == 1);
  PL_ASSERT(executor_context_);

  // Grab data from plan node.
  const planner::AbstractScan &node = GetPlanNode<planner::AbstractScan>();

  predicate_ = node.GetPredicate();

  column_ids_ = std::move(node.GetColumnIds());

  return true;
}

}  // namespace executor
}  // namespace peloton

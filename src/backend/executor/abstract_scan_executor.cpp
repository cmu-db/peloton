//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_scan_executor.cpp
//
// Identification: src/backend/executor/abstract_scan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/abstract_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"

#include "backend/common/logger.h"

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
  assert(children_.size() == 0 || children_.size() == 1);
  
  // for caching reason, executor_context_ can be null_ptr when first initialized.
  //assert(executor_context_);

  // Grab data from plan node.
  const planner::AbstractScan &node = GetPlanNode<planner::AbstractScan>();

  predicate_ = node.GetPredicate();
  // auto column_ids = node.GetColumnIds();

  column_ids_ = std::move(node.GetColumnIds());

  return true;
}

}  // namespace executor
}  // namespace peloton

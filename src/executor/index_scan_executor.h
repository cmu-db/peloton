/**
 * @brief Header file for index scan executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

#include <vector>

namespace nstore {
namespace executor {

class IndexScanExecutor : public AbstractExecutor {
  IndexScanExecutor(const IndexScanExecutor &) = delete;
  IndexScanExecutor& operator=(const IndexScanExecutor &) = delete;

 public:
  explicit IndexScanExecutor(planner::AbstractPlanNode *node,
                          Transaction *transaction);

 protected:
  bool DInit();

  bool DExecute();

 private:

  /** @brief Keeps track of current tile group id being scanned. */
  //oid_t current_tile_group_id_ = 0;

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result;
};

} // namespace executor
} // namespace nstore

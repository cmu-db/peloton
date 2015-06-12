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

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result;

  /** @brief Result itr */
  size_t result_itr;

  /** @brief Computed the result */
  bool done;
};

} // namespace executor
} // namespace nstore

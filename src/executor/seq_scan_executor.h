/**
 * @brief Header file for sequential scan executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "common/types.h"
#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {

class SeqScanExecutor : public AbstractExecutor {
  SeqScanExecutor(const SeqScanExecutor &) = delete;
  SeqScanExecutor& operator=(const SeqScanExecutor &) = delete;

 public:
  explicit SeqScanExecutor(planner::AbstractPlanNode *node);

 protected:
  bool DInit();

  bool DExecute();

 private:
  /** @brief Keeps track of current tile group id being scanned. */
  id_t current_tile_group_id_ = 0;
};

} // namespace executor
} // namespace nstore

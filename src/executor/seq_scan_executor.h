/**
 * @brief Header file for sequential scan executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

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
};

} // namespace executor
} // namespace nstore

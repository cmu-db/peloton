/**
 * @brief Header file for concat executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {

class ConcatExecutor : public AbstractExecutor {
 public:
  ConcatExecutor(const ConcatExecutor &) = delete;
  ConcatExecutor& operator=(const ConcatExecutor &) = delete;
  ConcatExecutor(ConcatExecutor &&) = delete;
  ConcatExecutor& operator=(ConcatExecutor &&) = delete;

  explicit ConcatExecutor(planner::AbstractPlanNode *node);

 protected:
  bool DInit();

  bool DExecute();
};

} // namespace executor
} // namespace nstore

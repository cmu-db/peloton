/**
 * @brief Header file for concat executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {
class LogicalTile;

class ConcatExecutor : public AbstractExecutor {
 public:
  explicit ConcatExecutor(const planner::AbstractPlanNode *node);
  ConcatExecutor(const ConcatExecutor &) = delete;
  ConcatExecutor& operator=(const ConcatExecutor &) = delete;
  ConcatExecutor(ConcatExecutor &&) = delete;
  ConcatExecutor& operator=(ConcatExecutor &&) = delete;

 protected:
  bool SubInit();

  LogicalTile *SubGetNextTile();
};

} // namespace executor
} // namespace nstore

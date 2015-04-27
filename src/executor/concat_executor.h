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
  ConcatExecutor(const ConcatExecutor &) = delete;
  ConcatExecutor& operator=(const ConcatExecutor &) = delete;

 public:
  explicit ConcatExecutor(planner::AbstractPlanNode *node);

 protected:
  bool SubInit();

  bool SubExecute();
};

} // namespace executor
} // namespace nstore

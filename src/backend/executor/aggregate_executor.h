/*-------------------------------------------------------------------------
 *
 * aggregate_executor.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/aggregate_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "executor/abstract_executor.h"

#include <vector>

namespace nstore {
namespace executor {

class AggregateExecutor : public AbstractExecutor {
 public:
  AggregateExecutor(const AggregateExecutor &) = delete;
  AggregateExecutor& operator=(const AggregateExecutor &) = delete;
  AggregateExecutor(AggregateExecutor &&) = delete;
  AggregateExecutor& operator=(AggregateExecutor &&) = delete;

  AggregateExecutor(planner::AbstractPlanNode *node);

  ~AggregateExecutor(){}

 protected:
  bool DInit();

  bool DExecute();

};

} // namespace executor
} // namespace nstore

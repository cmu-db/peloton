//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// append_executor.h
//
// Identification: src/backend/executor/append_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Append executor.
 * Trivially concatenate input tiles from the children.
 * No check on the schemas of children.
 */
class AppendExecutor : public AbstractExecutor {
 public:
  AppendExecutor(const AppendExecutor &) = delete;
  AppendExecutor &operator=(const AppendExecutor &) = delete;
  AppendExecutor(const AppendExecutor &&) = delete;
  AppendExecutor &operator=(const AppendExecutor &&) = delete;

  explicit AppendExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

 protected:
  bool DInit();
  bool DExecute();

 private:
  size_t cur_child_id_ = 0;
};
}
}

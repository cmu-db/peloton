//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// append_executor.h
//
// Identification: src/include/executor/append_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Append executor.
 * Trivially concatenate input tiles from the children.
 * No check on the schemas of children.
 *
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
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

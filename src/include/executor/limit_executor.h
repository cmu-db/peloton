//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_executor.h
//
// Identification: src/include/executor/limit_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"

namespace peloton {
namespace executor {

/**
 * TODO Currently, both limit and offset must be good numbers.
 * Postgres also allows stand-alone LIMIT and stand-alone OFFSET.
 * Need further change to accommodate it.
 *
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class LimitExecutor : public AbstractExecutor {
 public:
  LimitExecutor(const LimitExecutor &) = delete;
  LimitExecutor &operator=(const LimitExecutor &) = delete;
  LimitExecutor(const LimitExecutor &&) = delete;
  LimitExecutor &operator=(const LimitExecutor &&) = delete;

  explicit LimitExecutor(const planner::AbstractPlan *node,
                         ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Number of tuples skipped. */
  size_t num_skipped_ = 0;

  /** @brief Number of tuples returned. */
  size_t num_returned_ = 0;
};

}  // namespace executor
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.h
//
// Identification: src/include/executor/insert_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"

namespace peloton {
namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class InsertExecutor : public AbstractExecutor {
 public:
  InsertExecutor(const InsertExecutor &) = delete;
  InsertExecutor &operator=(const InsertExecutor &) = delete;
  InsertExecutor(InsertExecutor &&) = delete;
  InsertExecutor &operator=(InsertExecutor &&) = delete;

  explicit InsertExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  bool done_ = false;
};

}  // namespace executor
}  // namespace peloton

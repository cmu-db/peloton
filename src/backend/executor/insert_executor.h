//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_executor.h
//
// Identification: src/backend/executor/insert_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/abstract_executor.h"

#include <vector>

namespace peloton {
namespace executor {

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

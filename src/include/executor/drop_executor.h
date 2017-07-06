//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_executor.h
//
// Identification: src/include/executor/drop_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"
#include "planner/drop_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

class DropExecutor : public AbstractExecutor {
 public:
  DropExecutor(const DropExecutor &) = delete;
  DropExecutor &operator=(const DropExecutor &) = delete;
  DropExecutor(DropExecutor &&) = delete;
  DropExecutor &operator=(DropExecutor &&) = delete;

  DropExecutor(const planner::AbstractPlan *node,
               ExecutorContext *executor_context);

  ~DropExecutor() {}

 protected:
  bool DInit();

  bool DExecute();

 private:
  ExecutorContext *context;
};

}  // namespace executor
}  // namespace peloton

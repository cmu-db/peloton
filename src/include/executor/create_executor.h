//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_executor.h
//
// Identification: src/include/executor/create_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"
#include "planner/create_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

class CreateExecutor : public AbstractExecutor {
 public:
  CreateExecutor(const CreateExecutor &) = delete;
  CreateExecutor &operator=(const CreateExecutor &) = delete;
  CreateExecutor(CreateExecutor &&) = delete;
  CreateExecutor &operator=(CreateExecutor &&) = delete;

  CreateExecutor(const planner::AbstractPlan *node,
                 ExecutorContext *executor_context);

  ~CreateExecutor() {}

 protected:
  bool DInit();

  bool DExecute();

 private:
  ExecutorContext *context;
};

}  // namespace executor
}  // namespace peloton

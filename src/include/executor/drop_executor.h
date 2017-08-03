//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_executor.h
//
// Identification: src/include/executor/drop_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace planner {
class AbstractPlan;
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
  ExecutorContext *context_;

};

}  // namespace executor
}  // namespace peloton

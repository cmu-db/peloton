//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_executor.h
//
// Identification: src/include/executor/create_executor.h
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
  ExecutorContext *context_;

  // Abstract Pool to hold strings
  std::unique_ptr<type::AbstractPool> pool_;

};

}  // namespace executor
}  // namespace peloton

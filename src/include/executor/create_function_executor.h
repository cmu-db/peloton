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
#include "planner/create_function_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

class CreateFunctionExecutor : public AbstractExecutor {
 public:
	CreateFunctionExecutor(const CreateFunctionExecutor &) = delete;
	CreateFunctionExecutor &operator=(const CreateFunctionExecutor &) = delete;
	CreateFunctionExecutor(CreateFunctionExecutor &&) = delete;
	CreateFunctionExecutor &operator=(CreateFunctionExecutor &&) = delete;

	CreateFunctionExecutor(const planner::AbstractPlan *node,
                 ExecutorContext *executor_context);

  	~CreateFunctionExecutor() {}

 protected:
  bool DInit();

  bool DExecute();

 private:
  ExecutorContext *context;

};

}  // namespace executor
}  // namespace peloton

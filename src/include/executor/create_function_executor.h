//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_executor.h
//
// Identification: src/include/executor/create_function_executor.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}  // namespace storage

namespace executor {

class CreateFunctionExecutor : public AbstractExecutor {
 public:
  /// Constructor
  CreateFunctionExecutor(const planner::AbstractPlan *node,
                         ExecutorContext *executor_context);

  /// This class cannot be copied or move-constructed
  DISALLOW_COPY_AND_MOVE(CreateFunctionExecutor);

 protected:
  bool DInit() override;

  bool DExecute() override;
};

}  // namespace executor
}  // namespace peloton

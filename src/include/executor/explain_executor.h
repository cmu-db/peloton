//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// explain_executor.h
//
// Identification: src/include/executor/explain_executor.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"
#include "planner/explain_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

class ExplainExecutor : public AbstractExecutor {
 public:
  ExplainExecutor(const ExplainExecutor &) = delete;
  ExplainExecutor &operator=(const ExplainExecutor &) = delete;
  ExplainExecutor(ExplainExecutor &&) = delete;
  ExplainExecutor &operator=(ExplainExecutor &&) = delete;

  ExplainExecutor(const planner::AbstractPlan *node,
                  ExecutorContext *executor_context)
      : AbstractExecutor(node, executor_context) {}

  ~ExplainExecutor() {}

 protected:
  bool DInit();

  bool DExecute();
};

}  // namespace executor
}  // namespace peloton

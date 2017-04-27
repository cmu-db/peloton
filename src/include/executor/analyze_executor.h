//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_executor.h
//
// Identification: src/include/executor/analyze_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"
#include "planner/analyze_plan.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

class AnalyzeExecutor : public AbstractExecutor {
 public:
	AnalyzeExecutor(const AnalyzeExecutor &) = delete;
	AnalyzeExecutor &operator=(const AnalyzeExecutor &) = delete;
	AnalyzeExecutor(AnalyzeExecutor &&) = delete;
	AnalyzeExecutor &operator=(AnalyzeExecutor &&) = delete;

	AnalyzeExecutor(const planner::AbstractPlan *node,
                 ExecutorContext *executor_context);

  ~AnalyzeExecutor() {}

 protected:
  bool DInit();

  bool DExecute();

 private:
  ExecutorContext* executor_context_;
};

}  // namespace executor
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_executor.h
//
// Identification: src/include/executor/alter_executor.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_context.h"
#include "executor/abstract_executor.h"
#include "planner/alter_plan.h"
#include "planner/rename_plan.h"

namespace peloton {

namespace planner {
class AbstractPlan;
}

namespace executor {

class AlterExecutor : public AbstractExecutor {
 public:
  AlterExecutor(const AlterExecutor &) = delete;
  AlterExecutor &operator=(const AlterExecutor &) = delete;
  AlterExecutor(AlterExecutor &&) = delete;
  AlterExecutor &operator=(AlterExecutor &&) = delete;

  AlterExecutor(const planner::AbstractPlan *node,
                ExecutorContext *executor_context);

  ~AlterExecutor() {}

 protected:
  bool DInit();

  bool DExecute();

  bool RenameColumn(const planner::RenamePlan &node,
                    concurrency::TransactionContext *txn);
};

}  // executor

}  // namespace peloton

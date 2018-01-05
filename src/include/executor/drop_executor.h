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

#include "concurrency/transaction_context.h"
#include "executor/abstract_executor.h"
#include "planner/drop_plan.h"

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

  bool DropDatabase(const planner::DropPlan &node,
                    concurrency::TransactionContext *txn);

  bool DropTable(const planner::DropPlan &node,
                 concurrency::TransactionContext *txn);

  bool DropTrigger(const planner::DropPlan &node,
                   concurrency::TransactionContext *txn);

  bool DropIndex(const planner::DropPlan &node,
                 concurrency::TransactionContext *txn);

 private:
  ExecutorContext *context_;
};

}  // namespace executor
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_executor.cpp
//
// Identification: src/executor/alter_executor.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/alter_executor.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "executor/executor_context.h"

namespace peloton {
namespace executor {

// Constructor for alter table executor
AlterExecutor::AlterExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

// Initialize executor
// Nothing to initialize for now
bool AlterExecutor::DInit() {
  LOG_TRACE("Initializing Alter Executer...");

  LOG_TRACE("Alter Executor initialized!");
  return true;
}

bool AlterExecutor::DExecute() {
  LOG_TRACE("Executing Alter...");
  bool result = false;
  const planner::RenamePlan &node = GetPlanNode<planner::RenamePlan>();
  auto current_txn = executor_context_->GetTransaction();
  PlanNodeType plan_node_type = node.GetPlanNodeType();
  if (plan_node_type == PlanNodeType::RENAME) {
    result = RenameColumn(node, current_txn);
  } else if (plan_node_type == PlanNodeType::ALTER) {
    LOG_TRACE("Will perform alter table operations");
  } else {
    throw NotImplementedException(
        StringUtil::Format("Plan node type not supported, %s",
                           PlanNodeTypeToString(plan_node_type).c_str()));
  }

  return result;
}

bool AlterExecutor::RenameColumn(
    const peloton::planner::RenamePlan &node,
    peloton::concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  auto table_name = node.GetTableName();
  auto new_column_name = node.GetNewName();
  auto old_column_name = node.GetOldName();

  ResultType result = catalog::Catalog::GetInstance()->RenameColumn(
      database_name, table_name, old_column_name, new_column_name, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Rename column succeeded!");

    // Add on succeed logic if necessary
    executor_context_->num_processed = 1;
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

}  // executor
}  // peloton

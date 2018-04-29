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
    : AbstractExecutor(node, executor_context),
      isAlter_(!reinterpret_cast<const planner::AlterPlan *>(node)->IsRename()) {}

// Initialize executor
// Nothing to initialize for now
bool AlterExecutor::DInit() {
  LOG_TRACE("Initializing Alter Executer...");

  LOG_TRACE("Alter Executor initialized!");
  return true;
}

bool AlterExecutor::DExecute() {
  LOG_TRACE("Executing Drop...");
  bool result = false;
  if (!isAlter_) {
    const planner::AlterPlan &node = GetPlanNode<planner::AlterPlan>();
    auto current_txn = executor_context_->GetTransaction();
    result = RenameColumn(node, current_txn);
  } else {
    const planner::AlterPlan &node = GetPlanNode<planner::AlterPlan>();
    auto current_txn = executor_context_->GetTransaction();
    AlterType type = node.GetAlterTableType();
    switch (type) {
      case AlterType::DROP:
        result = DropColumn(node, current_txn);
        break;
      default:
        throw NotImplementedException(StringUtil::Format(
            "Alter Type not supported, %s", AlterTypeToString(type).c_str()));
    }
  }

  return result;
}

bool AlterExecutor::RenameColumn(
    const peloton::planner::AlterPlan &node,
    peloton::concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  auto table_name = node.GetTableName();
  auto new_column_name = node.GetNewName();
  auto old_column_name = node.GetOldName();

  std::vector<std::string> old_names = {old_column_name};
  std::vector<std::string> names = {new_column_name};
  ResultType result = catalog::Catalog::GetInstance()->ChangeColumnName(
      database_name, table_name, old_names, names, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Rename column succeeded!");

    // TODO Add on succeed logic if necessary
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

bool AlterExecutor::DropColumn(const peloton::planner::AlterPlan &node,
                               peloton::concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  auto table_name = node.GetTableName();
  auto drop_columns = node.GetDroppedColumns();

  ResultType result = catalog::Catalog::GetInstance()->DropColumn(
      database_name, table_name, drop_columns, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Drop column succeed!");

    // TODO Add on succeed logic if necessary
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

}  // executor
}  // peloton

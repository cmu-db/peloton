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
#include "catalog/table_catalog.h"
#include "common/logger.h"
#include "executor/executor_context.h"
#include "storage/data_table.h"

namespace peloton {
namespace executor {

// Constructor for alter table executor
AlterExecutor::AlterExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

// Initialize executor
// Nothing to initialize for now
bool AlterExecutor::DInit() {
  LOG_TRACE("Initializing Alter Executor...");

  LOG_TRACE("Alter Executor initialized!");
  return true;
}

bool AlterExecutor::DExecute() {
  LOG_TRACE("Executing Alter...");
  bool result;
  const planner::AlterPlan &node = GetPlanNode<planner::AlterPlan>();
  auto current_txn = executor_context_->GetTransaction();
  AlterType type = node.GetAlterTableType();

  // TODO: grab per-table lock before execution.
  switch (type) {
    case AlterType::RENAME:
      result = RenameColumn(node, current_txn);
      break;
    case AlterType::ALTER:
      result = AlterTable(node, current_txn);
      break;
    default:
      throw NotImplementedException(StringUtil::Format(
          "Alter Type not supported, %s", AlterTypeToString(type).c_str()));
  }
  return result;
}

bool AlterExecutor::RenameColumn(
    const peloton::planner::AlterPlan &node,
    peloton::concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  auto table_name = node.GetTableName();
  auto schema_name = node.GetSchemaName();
  auto new_column_name = node.GetNewName();
  auto old_column_name = node.GetOldName();

  ResultType result = catalog::Catalog::GetInstance()->RenameColumn(
      database_name, table_name, old_column_name, new_column_name, schema_name,
      txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Rename column succeeded!");

    executor_context_->num_processed = 1;
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

bool AlterExecutor::AlterTable(const peloton::planner::AlterPlan &node,
                               peloton::concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  auto table_name = node.GetTableName();
  auto schema_name = node.GetSchemaName();
  auto table_catalog_obj = catalog::Catalog::GetInstance()->GetTableObject(
      database_name, schema_name, table_name, txn);
  oid_t database_oid = table_catalog_obj->GetDatabaseOid();
  oid_t table_oid = table_catalog_obj->GetTableOid();

  auto old_table = catalog::Catalog::GetInstance()->GetTableWithName(
      database_name, schema_name, table_name, txn);
  auto old_schema = old_table->GetSchema();
  std::vector<oid_t> column_ids;

  // Step 1: remove drop columns from old schema
  for (oid_t i = 0; i < old_schema->GetColumnCount(); ++i) {
    bool is_found = false;
    for (auto drop_column : node.GetDroppedColumns()) {
      if (old_schema->GetColumn(i).GetName() == drop_column) {
        is_found = true;
      }
    }
    if (!is_found) {
      column_ids.push_back(i);
    }
  }
  // Check if dropped column exists
  if (column_ids.size() + node.GetDroppedColumns().size() !=
      old_schema->GetColumnCount()) {
    LOG_TRACE("Dropped column not exists");
    txn->SetResult(ResultType::FAILURE);
    return false;
  }
  std::unique_ptr<catalog::Schema> temp_schema(
      catalog::Schema::CopySchema(old_schema, column_ids));
  auto columns = temp_schema->GetColumns();
  // Step 2: change column type if exists
  for (auto &change_col : node.GetChangedTypeColumns().get()->GetColumns()) {
    bool is_found = false;
    oid_t i = 0;
    for (; i < columns.size(); ++i) {
      if (columns[i].GetName() == change_col.GetName()) {
        is_found = true;
        break;
      }
    }
    if (!is_found) {
      LOG_TRACE("Change column type failed: Column %s does not exists",
                change_col.GetName().c_str());
      txn->SetResult(ResultType::FAILURE);
      return false;
    } else {
      columns[i] = std::move(change_col);
    }
  }

  // Step 3: append add column to new schema
  // construct add column schema
  std::vector<catalog::Column> add_columns;
  for (auto column : node.GetAddedColumns()->GetColumns()) {
    add_columns.push_back(column);
  }
  // Check if added column exists
  for (auto new_column : add_columns) {
    for (auto old_column : old_schema->GetColumns()) {
      if (new_column.GetName() == old_column.GetName()) {
        LOG_TRACE("Add column failed: Column %s already exists",
                  new_column.GetName().c_str());
        txn->SetResult(ResultType::FAILURE);
        return false;
      }
    }
  }
  columns.insert(columns.end(), add_columns.begin(), add_columns.end());
  // Construct new schema
  std::unique_ptr<catalog::Schema> new_schema(new catalog::Schema(columns));

  // Copy and replace table content to new schema in catalog
  ResultType result = catalog::Catalog::GetInstance()->AlterTable(
      database_oid, table_oid, schema_name, new_schema, txn);
  txn->SetResult(result);

  switch (txn->GetResult()) {
    case ResultType::SUCCESS:
      LOG_TRACE("Alter table succeed!");

      executor_context_->num_processed = 1;
      break;
    case ResultType::FAILURE:
      LOG_TRACE("Alter table failed!");

      break;
    default:
      LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
      break;
  }
  return false;
}

}  // executor
}  // peloton

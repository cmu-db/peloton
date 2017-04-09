//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_executor.cpp
//
// Identification: src/executor/create_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/create_executor.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "executor/executor_context.h"
#include "planner/create_plan.h"

namespace peloton {
namespace executor {

// Constructor for drop executor
CreateExecutor::CreateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context_ = executor_context;
}

// Initialize executer
// Nothing to initialize now
bool CreateExecutor::DInit() {
  LOG_TRACE("Initializing Create Executer...");
  LOG_TRACE("Create Executer initialized!");
  return true;
}

bool CreateExecutor::DExecute() {
  LOG_TRACE("Executing Create...");
  const planner::CreatePlan &node = GetPlanNode<planner::CreatePlan>();
  auto current_txn = context_->GetTransaction();

  // Check if query was for creating table
  if (node.GetCreateType() == CreateType::TABLE) {
    std::string table_name = node.GetTableName();
    auto database_name = node.GetDatabaseName();
    std::unique_ptr<catalog::Schema> schema(node.GetSchema());

    ResultType result = catalog::Catalog::GetInstance()->CreateTable(
        database_name, table_name, std::move(schema), current_txn);
    current_txn->SetResult(result);

    if (current_txn->GetResult() == ResultType::SUCCESS) {
      LOG_TRACE("Creating table succeeded!");
    } else if (current_txn->GetResult() == ResultType::FAILURE) {
      LOG_TRACE("Creating table failed!");
    } else {
      LOG_TRACE("Result is: %s",
                ResultTypeToString(current_txn->GetResult()).c_str());
    }
  }

  // Check if query was for creating index
  if (node.GetCreateType() == CreateType::INDEX) {
    std::string table_name = node.GetTableName();
    std::string index_name = node.GetIndexName();
    bool unique_flag = node.IsUnique();
    IndexType index_type = node.GetIndexType();

    auto index_attrs = node.GetIndexAttributes();

    ResultType result = catalog::Catalog::GetInstance()->CreateIndex(
        DEFAULT_DB_NAME, table_name, index_attrs, index_name, unique_flag,
        index_type, current_txn);
    current_txn->SetResult(result);

    if (current_txn->GetResult() == ResultType::SUCCESS) {
      LOG_TRACE("Creating table succeeded!");
    } else if (current_txn->GetResult() == ResultType::FAILURE) {
      LOG_TRACE("Creating table failed!");
    } else {
      LOG_TRACE("Result is: %s",
                ResultTypeToString(current_txn->GetResult()).c_str());
    }
  }

  // Check if query was for creating trigger
  if (node.GetCreateType() == CreateType::TRIGGER) {
    std::string table_name = node.GetTableName();
    std::string trigger_name = node.GetTriggerName();
    std::unique_ptr<catalog::Schema> schema(node.GetSchema());

    // TODO: add trigger into catalog (waiting for the changes of catalog API)
    ResultType result = ResultType::SUCCESS;
    current_txn->SetResult(result);

    if (current_txn->GetResult() == ResultType::SUCCESS) {
      LOG_TRACE("Creating trigger succeeded!");
    } else if (current_txn->GetResult() == ResultType::FAILURE) {
      LOG_TRACE("Creating trigger failed!");
    } else {
      LOG_TRACE("Result is: %s", ResultTypeToString(
        current_txn->GetResult()).c_str());
    }
  }

  return false;
}

}  // namespace executor
}  // namespace peloton

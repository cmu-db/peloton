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
#include "trigger/trigger.h"
#include "storage/data_table.h"

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
    LOG_INFO("enter CreateType::TRIGGER");
    std::string database_name = node.GetDatabaseName();
    std::string table_name = node.GetTableName();
    std::string trigger_name = node.GetTriggerName();

    trigger::Trigger newTrigger(node);

    oid_t database_oid = catalog::DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, current_txn);
    oid_t table_oid = catalog::TableCatalog::GetInstance()->GetTableOid(table_name, database_oid, current_txn);

    // durable trigger: insert the information of this trigger in the trigger catalog table
    auto time_stamp = type::ValueFactory::GetTimestampValue(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    CopySerializeOutput output;
    newTrigger.SerializeWhen(output, table_oid, current_txn);
    auto when = type::ValueFactory::GetVarbinaryValue(
        (const unsigned char*)output.Data(),
        (int32_t)output.Size(), true);

    catalog::TriggerCatalog::GetInstance()->InsertTrigger(
        table_oid, trigger_name,
        newTrigger.GetTriggerType(),
        newTrigger.GetFuncname(),
        newTrigger.GetArgs(),
        when,
        time_stamp,
        pool_.get(), current_txn);
    // ask target table to update its trigger list variable
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(database_name,
                                                          table_name);
    target_table->UpdateTriggerListFromCatalog(current_txn);

    // hardcode SUCCESS result for current_txn
    current_txn->SetResult(ResultType::SUCCESS);
  }

  return false;
}

}  // namespace executor
}  // namespace peloton

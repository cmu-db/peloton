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
#include "commands/trigger.h"
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
    // std::unique_ptr<catalog::Schema> schema(node.GetSchema());

    commands::Trigger newTrigger(node);

    // // new approach: add trigger to the data_table instance directly
    // storage::DataTable *target_table =
    //     catalog::Catalog::GetInstance()->GetTableWithName(database_name,
    //                                                       table_name);
    // target_table->AddTrigger(newTrigger);


    oid_t database_oid = catalog::DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, current_txn);
    oid_t table_oid = catalog::TableCatalog::GetInstance()->GetTableOid(table_name, database_oid, current_txn);

    //debug for check newtrigger's trigger_when has been settled or not.
    LOG_INFO("in CreateExecutor::DExecute==========================");
    if (newTrigger.GetTriggerWhen() == nullptr) {
      LOG_INFO("new trigger's trigger_when is nullptr");
    } else {
      LOG_INFO("new trigger's trigger_when is not nullptr");
      auto trigger_when_t = newTrigger.GetTriggerWhen();
      if (trigger_when_t->GetExpressionType() == ExpressionType::COMPARE_NOTEQUAL) {
        LOG_INFO("type is COMPARE_NOTEQUAL");
      } else {
        LOG_INFO("type is not COMPARE_NOTEQUAL");
      }

      std::string serialize_when = newTrigger.SerializeWhen(table_oid, current_txn);
      if (serialize_when == "") {
        LOG_INFO("serialize_when is empty str");
      } else {
        LOG_INFO("serialize_when is %s", serialize_when.c_str());
      }
    }

    // durable trigger: insert the information of this trigger in the trigger catalog table
    auto time_since_epoch = std::chrono::system_clock::now().time_since_epoch();
    auto time_stamp = std::chrono::duration_cast<std::chrono::seconds>(
                        time_since_epoch).count();
    LOG_INFO("1");
    catalog::TriggerCatalog::GetInstance()->InsertTrigger(trigger_name, database_oid, table_oid,
                    newTrigger.GetTriggerType(), newTrigger.SerializeWhen(table_oid, current_txn),
                    newTrigger.GetFuncname(), newTrigger.GetArgs(),
                    time_stamp, pool_.get(), current_txn);
    LOG_INFO("2");
    // ask target table to update its trigger list variable
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(database_name,
                                                          table_name);
    target_table->UpdateTriggerListFromCatalog(current_txn);



    // // debug:
    // auto trigger_list = catalog::TriggerCatalog::GetInstance()->GetTriggersByType(database_oid, table_oid,
    //                           newTrigger.GetTriggerType(), current_txn);
    // if (trigger_list == nullptr) {
    //   LOG_INFO("nullptr");
    // } else {
    //   LOG_INFO("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
    // }

    // LOG_INFO("trigger type=%d", newTrigger.GetTriggerType());
    // LOG_INFO("3");

    // if (current_txn->GetResult() == ResultType::SUCCESS) {
    //   LOG_TRACE("Creating trigger succeeded!");
    // } else if (current_txn->GetResult() == ResultType::FAILURE) {
    //   LOG_TRACE("Creating trigger failed!");
    // } else {
    //   LOG_TRACE("Result is: %s", ResultTypeToString(
    //     current_txn->GetResult()).c_str());
    // }

    // hardcode SUCCESS result for current_txn
    current_txn->SetResult(ResultType::SUCCESS);
  }

  return false;
}

}  // namespace executor
}  // namespace peloton

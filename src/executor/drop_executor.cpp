//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_executor.cpp
//
// Identification: src/executor/drop_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/drop_executor.h"

#include "catalog/catalog.h"
#include "catalog/trigger_catalog.h"
#include "common/logger.h"
#include "concurrency/transaction_context.h"
#include "executor/executor_context.h"
#include "planner/drop_plan.h"

namespace peloton {
namespace executor {

// Constructor for drop executor
DropExecutor::DropExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context_ = executor_context;
}

// Initialize executer
// Nothing to initialize for now
bool DropExecutor::DInit() {
  LOG_TRACE("Initializing Drop Executer...");

  LOG_TRACE("Create Executer initialized!");
  return true;
}

bool DropExecutor::DExecute() {
  LOG_TRACE("Executing Drop...");
  const planner::DropPlan &node = GetPlanNode<planner::DropPlan>();
  DropType dropType = node.GetDropType();
  switch (dropType) {
    case DropType::DB: {
      auto database_name = node.GetDatabaseName();
      auto current_txn = context_->GetTransaction();
      if (node.IsMissing()) {
        try {
          auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
              database_name, current_txn);
        } catch (CatalogException &e) {
          LOG_TRACE("Database %s does not exist.", database_name.c_str());
          return false;
        }
      }

      ResultType result = catalog::Catalog::GetInstance()->DropDatabaseWithName(
          database_name, current_txn);
      current_txn->SetResult(result);

      if (current_txn->GetResult() == ResultType::SUCCESS) {
        LOG_TRACE("Dropping database succeeded!");
      } else {
        LOG_TRACE("Result is: %s",
                  ResultTypeToString(current_txn->GetResult()).c_str());
      }
      break;
    }
    case DropType::TABLE: {
      auto database_name = node.GetDatabaseName();
      auto table_name = node.GetTableName();
      auto current_txn = context_->GetTransaction();

      if (node.IsMissing()) {
        try {
          auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
              database_name, table_name, current_txn);
        } catch (CatalogException &e) {
          LOG_TRACE("Table %s does not exist.", table_name.c_str());
          return false;
        }
      }

      ResultType result = catalog::Catalog::GetInstance()->DropTable(
          database_name, table_name, current_txn);
      current_txn->SetResult(result);

      if (current_txn->GetResult() == ResultType::SUCCESS) {
        LOG_TRACE("Dropping table succeeded!");
      } else {
        LOG_TRACE("Result is: %s",
                  ResultTypeToString(current_txn->GetResult()).c_str());
      }
      break;
    }
    case DropType::TRIGGER: {
      auto database_name = node.GetDatabaseName();
      std::string table_name = node.GetTableName();
      std::string trigger_name = node.GetTriggerName();

      auto current_txn = context_->GetTransaction();

      ResultType result = catalog::TriggerCatalog::GetInstance().DropTrigger(
          database_name, table_name, trigger_name, current_txn);
      current_txn->SetResult(result);

      if (current_txn->GetResult() == ResultType::SUCCESS) {
        LOG_TRACE("Dropping trigger succeeded!");
      } else if (current_txn->GetResult() == ResultType::FAILURE &&
                 node.IsMissing()) {
        current_txn->SetResult(ResultType::SUCCESS);
        LOG_TRACE("Dropping trigger Succeeded!");
      } else if (current_txn->GetResult() == ResultType::FAILURE &&
                 !node.IsMissing()) {
        LOG_TRACE("Dropping trigger Failed!");
      } else {
        LOG_TRACE("Result is: %s",
                  ResultTypeToString(current_txn->GetResult()).c_str());
      }
      break;
    }
    default: {
      throw NotImplementedException(
          StringUtil::Format("Drop type %d not supported yet.\n", dropType));
    }
  }

  return false;
}

}  // namespace executor
}  // namespace peloton

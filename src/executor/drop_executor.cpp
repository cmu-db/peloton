//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_executor.cpp
//
// Identification: src/executor/drop_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "executor/drop_executor.h"
#include "executor/executor_context.h"
#include "common/logger.h"
#include "catalog/catalog.h"

namespace peloton {
namespace executor {

// Constructor for drop executor
DropExecutor::DropExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context = executor_context;
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
    case DropType::TABLE: {
      std::string table_name = node.GetTableName();

      auto current_txn = context->GetTransaction();

      ResultType result = catalog::Catalog::GetInstance()->DropTable(
        DEFAULT_DB_NAME, table_name, current_txn);
      current_txn->SetResult(result);

      if (current_txn->GetResult() == ResultType::SUCCESS) {
        LOG_TRACE("Dropping table succeeded!");
      } else if (current_txn->GetResult() == ResultType::FAILURE &&
        node.IsMissing()) {
        current_txn->SetResult(ResultType::SUCCESS);
        LOG_TRACE("Dropping table Succeeded!");
      } else if (current_txn->GetResult() == ResultType::FAILURE &&
        !node.IsMissing()) {
        LOG_TRACE("Dropping table Failed!");
      } else {
        LOG_TRACE("Result is: %s", ResultTypeToString(
          current_txn->GetResult()).c_str());
      }
      break;
    }
    case DropType::TRIGGER: {
      std::string table_name = node.GetTableName();
      std::string trigger_name = node.GetTriggerName();

      auto current_txn = context->GetTransaction();

      ResultType result = catalog::TriggerCatalog::GetInstance()->DropTrigger(
        DEFAULT_DB_NAME, table_name, trigger_name, current_txn);
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
        LOG_TRACE("Result is: %s", ResultTypeToString(
          current_txn->GetResult()).c_str());
      }
    }
    default: {
      throw NotImplementedException(StringUtil::Format(
        "Drop type %d not supported yet.\n", dropType));
    }
  }

  return false;
}
}
}

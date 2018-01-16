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
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/trigger_catalog.h"
#include "common/logger.h"
#include "common/statement_cache_manager.h"
#include "executor/executor_context.h"

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
  bool result;
  const planner::DropPlan &node = GetPlanNode<planner::DropPlan>();
  auto current_txn = context_->GetTransaction();
  DropType dropType = node.GetDropType();
  switch (dropType) {
    case DropType::DB: {
      result = DropDatabase(node, current_txn);
      break;
    }
    case DropType::TABLE: {
      result = DropTable(node, current_txn);
      break;
    }
    case DropType::TRIGGER: {
      result = DropTrigger(node, current_txn);
      break;
    }
    case DropType::INDEX: {
      result = DropIndex(node, current_txn);
      break;
    }
    default: {
      throw NotImplementedException(
          StringUtil::Format("Drop type %d not supported yet.\n", dropType));
    }
  }

  return result;
}

bool DropExecutor::DropDatabase(const planner::DropPlan &node,
                                concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();

  if (node.IsMissing()) {
    try {
      auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
          database_name, txn);
    } catch (CatalogException &e) {
      LOG_TRACE("Database %s does not exist.", database_name.c_str());
      return false;
    }
  }

  auto database_object =
      catalog::Catalog::GetInstance()->GetDatabaseObject(database_name, txn);

  ResultType result =
      catalog::Catalog::GetInstance()->DropDatabaseWithName(database_name, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Dropping database succeeded!");

    if (StatementCacheManager::GetStmtCacheManager().get()) {
      std::set<oid_t> table_ids;
      auto table_objects = database_object->GetTableObjects(false);
      for (auto it : table_objects) {
        table_ids.insert(it.second->GetTableOid());
      }
      StatementCacheManager::GetStmtCacheManager()->InvalidateTableOids(
          table_ids);
    }
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

bool DropExecutor::DropTable(const planner::DropPlan &node,
                             concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  auto table_name = node.GetTableName();

  if (node.IsMissing()) {
    try {
      auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
          database_name, table_name, txn);
    } catch (CatalogException &e) {
      LOG_TRACE("Table %s does not exist.", table_name.c_str());
      return false;
    }
  }

  ResultType result = catalog::Catalog::GetInstance()->DropTable(
      database_name, table_name, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Dropping table succeeded!");

    if (StatementCacheManager::GetStmtCacheManager().get()) {
      oid_t table_id = catalog::Catalog::GetInstance()
                           ->GetTableObject(database_name, table_name, txn)
                           ->GetTableOid();
      StatementCacheManager::GetStmtCacheManager()->InvalidateTableOid(
          table_id);
    }
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

bool DropExecutor::DropTrigger(const planner::DropPlan &node,
                               concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  std::string table_name = node.GetTableName();
  LOG_DEBUG("database name: %s", database_name.c_str());
  LOG_DEBUG("table name: %s", table_name.c_str());
  std::string trigger_name = node.GetTriggerName();

  ResultType result = catalog::TriggerCatalog::GetInstance().DropTrigger(
      database_name, table_name, trigger_name, txn);
  txn->SetResult(result);
  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_DEBUG("Dropping trigger succeeded!");

    if (StatementCacheManager::GetStmtCacheManager().get()) {
      oid_t table_id = catalog::Catalog::GetInstance()
                           ->GetTableObject(database_name, table_name, txn)
                           ->GetTableOid();
      StatementCacheManager::GetStmtCacheManager()->InvalidateTableOid(
          table_id);
    }
  } else if (txn->GetResult() == ResultType::FAILURE && node.IsMissing()) {
    txn->SetResult(ResultType::SUCCESS);
    LOG_TRACE("Dropping trigger Succeeded!");
  } else if (txn->GetResult() == ResultType::FAILURE && !node.IsMissing()) {
    LOG_TRACE("Dropping trigger Failed!");
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

bool DropExecutor::DropIndex(const planner::DropPlan &node,
                             concurrency::TransactionContext *txn) {
  std::string index_name = node.GetIndexName();
  auto index_object =
      catalog::IndexCatalog::GetInstance()->GetIndexObject(index_name, txn);

  ResultType result = catalog::Catalog::GetInstance()->DropIndex(
      index_object->GetIndexOid(), txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    if (StatementCacheManager::GetStmtCacheManager().get()) {
      oid_t table_id = index_object->GetTableOid();
      StatementCacheManager::GetStmtCacheManager()->InvalidateTableOid(
          table_id);
    }
    LOG_TRACE("Dropping Index Succeeded! Index oid: %d",
              index_object->GetIndexOid());
  } else {
    LOG_TRACE("Dropping Index Failed!");
  }
  return false;
}

}  // namespace executor
}  // namespace peloton
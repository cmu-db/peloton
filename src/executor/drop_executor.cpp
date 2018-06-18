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
#include "catalog/index_catalog.h"
#include "catalog/system_catalogs.h"
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
    case DropType::SCHEMA: {
      result = DropSchema(node, current_txn);
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
  std::string database_name = node.GetDatabaseName();

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

bool DropExecutor::DropSchema(const planner::DropPlan &node,
                              concurrency::TransactionContext *txn) {
  std::string database_name = node.GetDatabaseName();
  std::string schema_name = node.GetSchemaName();

  ResultType result = catalog::Catalog::GetInstance()->DropSchema(
      database_name, schema_name, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_DEBUG("Dropping schema succeeded!");
    // add dropped table into StatementCacheManager
    if (StatementCacheManager::GetStmtCacheManager().get()) {
      std::set<oid_t> table_ids;
      auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
          database_name, txn);
      auto table_objects = database_object->GetTableObjects(schema_name);
      for (int i = 0; i < (int)table_objects.size(); i++) {
        table_ids.insert(table_objects[i]->GetTableOid());
      }
      StatementCacheManager::GetStmtCacheManager()->InvalidateTableOids(
          table_ids);
    }
  } else {
    LOG_DEBUG("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

bool DropExecutor::DropTable(const planner::DropPlan &node,
                             concurrency::TransactionContext *txn) {
  std::string database_name = node.GetDatabaseName();
  std::string schema_name = node.GetSchemaName();
  std::string table_name = node.GetTableName();

  if (node.IsMissing()) {
    try {
      auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
          database_name, schema_name, table_name, txn);
    } catch (CatalogException &e) {
      LOG_TRACE("Table %s does not exist.", table_name.c_str());
      return false;
    }
  }

  ResultType result = catalog::Catalog::GetInstance()->DropTable(
      database_name, schema_name, table_name, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Dropping table succeeded!");

    if (StatementCacheManager::GetStmtCacheManager().get()) {
      oid_t table_id =
          catalog::Catalog::GetInstance()
              ->GetTableObject(database_name, schema_name, table_name, txn)
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
  std::string database_name = node.GetDatabaseName();
  std::string schema_name = node.GetSchemaName();
  std::string table_name = node.GetTableName();
  std::string trigger_name = node.GetTriggerName();

  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      database_name, schema_name, table_name, txn);
  // drop trigger
  ResultType result =
      catalog::Catalog::GetInstance()
          ->GetSystemCatalogs(table_object->GetDatabaseOid())
          ->GetTriggerCatalog()
          ->DropTrigger(table_object->GetDatabaseOid(),
                        table_object->GetTableOid(), trigger_name, txn);
  txn->SetResult(result);
  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_DEBUG("Dropping trigger succeeded!");

    if (StatementCacheManager::GetStmtCacheManager().get()) {
      oid_t table_id = table_object->GetTableOid();
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
  std::string schema_name = node.GetSchemaName();
  auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
      node.GetDatabaseName(), txn);
  if (database_object == nullptr) {
    throw CatalogException("Index name " + index_name + " cannot be found");
  }

  auto pg_index = catalog::Catalog::GetInstance()
                      ->GetSystemCatalogs(database_object->GetDatabaseOid())
                      ->GetIndexCatalog();
  auto index_object = pg_index->GetIndexObject(index_name, schema_name, txn);
  if (index_object == nullptr) {
    throw CatalogException("Can't find index " + schema_name + "." +
                           index_name + " to drop");
  }
  // invoke directly using oid
  ResultType result = catalog::Catalog::GetInstance()->DropIndex(
      database_object->GetDatabaseOid(), index_object->GetIndexOid(), txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Dropping Index Succeeded! Index name: %s", index_name.c_str());
    if (StatementCacheManager::GetStmtCacheManager().get()) {
      oid_t table_id = index_object->GetTableOid();
      StatementCacheManager::GetStmtCacheManager()->InvalidateTableOid(
          table_id);
    }
  } else {
    LOG_TRACE("Dropping Index Failed!");
  }
  return false;
}

}  // namespace executor
}  // namespace peloton

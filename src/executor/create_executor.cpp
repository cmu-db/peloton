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
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "executor/executor_context.h"
#include "planner/create_plan.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "type/value_factory.h"

namespace peloton {
namespace executor {

// Constructor for drop executor
CreateExecutor::CreateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context_ = executor_context;
  pool_.reset(new type::EphemeralPool());
}

// Initialize executor
// Nothing to initialize now
bool CreateExecutor::DInit() {
  LOG_TRACE("Initialized CreateExecutor (nothing really) ...");
  return true;
}

bool CreateExecutor::DExecute() {
  LOG_TRACE("Executing Create...");
  const planner::CreatePlan &node = GetPlanNode<planner::CreatePlan>();

  UNUSED_ATTRIBUTE bool result = false;

  switch (node.GetCreateType()) {
    case CreateType::DB: {
      result = CreateDatabase(node);
      break;
    }
    // if query was for creating schema(namespace)
    case CreateType::SCHEMA: {
      result = CreateSchema(node);
      break;
    }
    // if query was for creating table
    case CreateType::TABLE: {
      result = CreateTable(node);
      break;
    }

    // if query was for creating index
    case CreateType::INDEX: {
      result = CreateIndex(node);
      break;
    }

    // if query was for creating trigger
    case CreateType::TRIGGER: {
      result = CreateTrigger(node);
      break;
    }

    default: {
      std::string create_type = CreateTypeToString(node.GetCreateType());
      LOG_ERROR("Not supported create type %s", create_type.c_str());
      std::string error =
          StringUtil::Format("Invalid Create type %s", create_type.c_str());
      throw ExecutorException(error);
    }
  }

  // FIXME: For some reason this only works correctly with the
  //        rest of the system if we return false.
  //        This is confusing and counterintuitive.
  return (false);
}

bool CreateExecutor::CreateDatabase(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  auto database_name = node.GetDatabaseName();
  // invoke logic within catalog.cpp
  ResultType result =
      catalog::Catalog::GetInstance()->CreateDatabase(txn, database_name);
  txn->SetResult(result);
  LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  return (true);
}

bool CreateExecutor::CreateSchema(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  auto database_name = node.GetDatabaseName();
  auto schema_name = node.GetSchemaName();
  // invoke logic within catalog.cpp
  ResultType result = catalog::Catalog::GetInstance()->CreateSchema(txn,
                                                                    database_name,
                                                                    schema_name);
  txn->SetResult(result);
  LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  return (true);
}

bool CreateExecutor::CreateTable(const planner::CreatePlan &node) {
  auto current_txn = context_->GetTransaction();
  std::string table_name = node.GetTableName();
  std::string schema_name = node.GetSchemaName();
  std::string database_name = node.GetDatabaseName();
  std::unique_ptr<catalog::Schema> schema(node.GetSchema());

  ResultType result = catalog::Catalog::GetInstance()->CreateTable(current_txn,
                                                                   database_name,
                                                                   schema_name,
                                                                   std::move(schema),
                                                                   table_name,
                                                                   false);
  current_txn->SetResult(result);

  if (current_txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Creating table succeeded!");
    auto catalog = catalog::Catalog::GetInstance();
    auto source_table = catalog->GetTableWithName(current_txn,
                                                  database_name,
                                                  schema_name,
                                                  table_name);
    // Add the primary key constraint
    if (node.HasPrimaryKey()) {
      auto pk = node.GetPrimaryKey();
      std::vector<oid_t> col_ids;
      for (auto col_name : pk.primary_key_cols) {
        oid_t col_id = source_table->GetSchema()->GetColumnID(col_name);
        if (col_id == INVALID_OID) {
          std::string error = StringUtil::Format(
              "Invalid key column name '%s.%s' for primary key '%s'",
              table_name.c_str(), col_name.c_str(), pk.constraint_name.c_str());
          throw ExecutorException(error);
        }
        col_ids.push_back(col_id);
      }
      PELOTON_ASSERT(col_ids.size() == pk.primary_key_cols.size());

      // Create the catalog object and shove it into the table
      catalog->AddPrimaryKeyConstraint(current_txn,
                                       source_table->GetDatabaseOid(),
                                       source_table->GetOid(),
                                       col_ids,
                                       pk.constraint_name);
    }

    // Add the unique constraint
    for (auto unique : node.GetUniques()) {
      std::vector<oid_t> col_ids;
      for (auto col_name : unique.unique_cols) {
        oid_t col_id = source_table->GetSchema()->GetColumnID(col_name);
        if (col_id == INVALID_OID) {
          std::string error = StringUtil::Format(
              "Invalid key column name '%s.%s' for unique '%s'",
              table_name.c_str(), col_name.c_str(),
              unique.constraint_name.c_str());
          throw ExecutorException(error);
        }
        col_ids.push_back(col_id);
      }
      PELOTON_ASSERT(col_ids.size() == unique.unique_cols.size());

      // Create the catalog object and shove it into the table
      catalog->AddUniqueConstraint(current_txn,
                                   source_table->GetDatabaseOid(),
                                   source_table->GetOid(),
                                   col_ids,
                                   unique.constraint_name);
    }

    // Add the foreign key constraint
    for (auto fk : node.GetForeignKeys()) {
      auto sink_table = catalog->GetTableWithName(current_txn,
                                                  database_name,
                                                  schema_name,
                                                  fk.sink_table_name);
      // Source Column Offsets
      std::vector<oid_t> source_col_ids;
      for (auto col_name : fk.foreign_key_sources) {
        oid_t col_id = source_table->GetSchema()->GetColumnID(col_name);
        if (col_id == INVALID_OID) {
          std::string error = StringUtil::Format(
              "Invalid source column name '%s.%s' for foreign key '%s'",
              table_name.c_str(), col_name.c_str(), fk.constraint_name.c_str());
          throw ExecutorException(error);
        }
        source_col_ids.push_back(col_id);
      }  // FOR
      PELOTON_ASSERT(source_col_ids.size() == fk.foreign_key_sources.size());

      // Sink Column Offsets
      std::vector<oid_t> sink_col_ids;
      for (auto col_name : fk.foreign_key_sinks) {
        oid_t col_id = sink_table->GetSchema()->GetColumnID(col_name);
        if (col_id == INVALID_OID) {
          std::string error = StringUtil::Format(
              "Invalid sink column name '%s.%s' for foreign key '%s'",
              sink_table->GetName().c_str(), col_name.c_str(),
              fk.constraint_name.c_str());
          throw ExecutorException(error);
        }
        sink_col_ids.push_back(col_id);
      }  // FOR
      PELOTON_ASSERT(sink_col_ids.size() == fk.foreign_key_sinks.size());

      // Create the catalog object and shove it into the table
      catalog->AddForeignKeyConstraint(current_txn,
                                       source_table->GetDatabaseOid(),
                                       source_table->GetOid(),
                                       source_col_ids,
                                       sink_table->GetOid(),
                                       sink_col_ids,
                                       fk.upd_action,
                                       fk.del_action,
                                       fk.constraint_name);
    }

    // Add the check constraint
    for (auto check : node.GetChecks()) {
      std::vector<oid_t> col_ids;
      for (auto col_name : check.check_cols) {
        oid_t col_id = source_table->GetSchema()->GetColumnID(col_name);
        if (col_id == INVALID_OID) {
          std::string error = StringUtil::Format(
              "Invalid key column name '%s.%s' for unique '%s'",
              table_name.c_str(), col_name.c_str(),
              check.constraint_name.c_str());
          throw ExecutorException(error);
        }
        col_ids.push_back(col_id);
      }
      PELOTON_ASSERT(col_ids.size() == check.check_cols.size());

      // Create the catalog object and shove it into the table
      catalog->AddCheckConstraint(current_txn,
                                  source_table->GetDatabaseOid(),
                                  source_table->GetOid(),
                                  col_ids, check.exp,
                                  check.constraint_name);
    }

  } else if (current_txn->GetResult() == ResultType::FAILURE) {
    LOG_TRACE("Creating table failed!");
  } else {
    LOG_TRACE("Result is: %s",
              ResultTypeToString(current_txn->GetResult()).c_str());
  }

  return (true);
}

bool CreateExecutor::CreateIndex(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  std::string database_name = node.GetDatabaseName();
  std::string schema_name = node.GetSchemaName();
  std::string table_name = node.GetTableName();
  std::string index_name = node.GetIndexName();
  bool unique_flag = node.IsUnique();
  IndexType index_type = node.GetIndexType();

  auto key_attrs = node.GetKeyAttrs();

  ResultType result = catalog::Catalog::GetInstance()->CreateIndex(txn,
                                                                   database_name,
                                                                   schema_name,
                                                                   table_name,
                                                                   index_name,
                                                                   key_attrs,
                                                                   unique_flag,
                                                                   index_type);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Creating table succeeded!");
  } else if (txn->GetResult() == ResultType::FAILURE) {
    LOG_TRACE("Creating table failed!");
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return (true);
}

bool CreateExecutor::CreateTrigger(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  std::string database_name = node.GetDatabaseName();
  std::string schema_name = node.GetSchemaName();
  std::string table_name = node.GetTableName();
  std::string trigger_name = node.GetTriggerName();

  trigger::Trigger newTrigger(node);
  auto table_object = catalog::Catalog::GetInstance()->GetTableCatalogEntry(txn,
                                                                            database_name,
                                                                            schema_name,
                                                                            table_name);

  // durable trigger: insert the information of this trigger in the trigger
  // catalog table
  auto time_stamp = type::ValueFactory::GetTimestampValue(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());

  CopySerializeOutput output;
  newTrigger.SerializeWhen(output, table_object->GetDatabaseOid(),
                           table_object->GetTableOid(), txn);
  auto when = type::ValueFactory::GetVarbinaryValue(
      (const unsigned char *)output.Data(), (int32_t)output.Size(), true);

  catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(table_object->GetDatabaseOid())
      ->GetTriggerCatalog()
      ->InsertTrigger(txn,
                      table_object->GetTableOid(),
                      trigger_name,
                      newTrigger.GetTriggerType(),
                      newTrigger.GetFuncname(),
                      newTrigger.GetArgs(),
                      when,
                      time_stamp,
                      pool_.get());
  // ask target table to update its trigger list variable
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(txn,
                                                        database_name,
                                                        schema_name,
                                                        table_name);
  target_table->UpdateTriggerListFromCatalog(txn);

  // hardcode SUCCESS result for txn
  txn->SetResult(ResultType::SUCCESS);

  // FIXME: We are missing failure logic here!
  // We seem to be assuming that we will always succeed
  // in installing the trigger.

  return (true);
}

}  // namespace executor
}  // namespace peloton

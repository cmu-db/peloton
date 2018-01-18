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
#include "catalog/foreign_key.h"
#include "catalog/trigger_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
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
  ResultType result = catalog::Catalog::GetInstance()->CreateDatabase(
      database_name, txn);
  txn->SetResult(result);
  LOG_TRACE("Result is: %s",
            ResultTypeToString(current_txn->GetResult()).c_str());
  return (true);
}

bool CreateExecutor::CreateTable(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  std::string table_name = node.GetTableName();
  auto database_name = node.GetDatabaseName();
  std::unique_ptr<catalog::Schema> schema(node.GetSchema());

  ResultType result = catalog::Catalog::GetInstance()->CreateTable(
      database_name, table_name, std::move(schema), txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Creating table succeeded!");

    // Add the foreign key constraint (or other multi-column constraints)
    if (node.GetForeignKeys().empty() == false) {
      auto catalog = catalog::Catalog::GetInstance();
      auto db = catalog->GetDatabaseWithName(database_name, txn);

      auto source_table = db->GetTableWithName(table_name);
      int count = 1;
      for (auto fk : node.GetForeignKeys()) {
        auto sink_table = db->GetTableWithName(fk.sink_table_name);

        // Source Column Offsets
        std::vector<oid_t> source_col_ids;
        for (auto col_name : fk.foreign_key_sources) {
          oid_t col_id = source_table->GetSchema()->GetColumnID(col_name);
          if (col_id == INVALID_OID) {
            std::string error = StringUtil::Format(
                "Invalid source column name '%s.%s' for foreign key '%s'",
                source_table->GetName().c_str(), col_name.c_str(),
                fk.constraint_name.c_str());
            throw ExecutorException(error);
          }
          source_col_ids.push_back(col_id);
        }  // FOR
        PL_ASSERT(source_col_ids.size() == fk.foreign_key_sources.size());

        // Sink Column Offsets
        std::vector<oid_t> sink_col_ids;
        for (auto col_name : fk.foreign_key_sinks) {
          oid_t col_id = source_table->GetSchema()->GetColumnID(col_name);
          if (col_id == INVALID_OID) {
            std::string error = StringUtil::Format(
                "Invalid sink column name '%s.%s' for foreign key '%s'",
                sink_table->GetName().c_str(), col_name.c_str(),
                fk.constraint_name.c_str());
            throw ExecutorException(error);
          }
          sink_col_ids.push_back(col_id);
        }  // FOR
        PL_ASSERT(sink_col_ids.size() == fk.foreign_key_sinks.size());

        // Create the catalog object and shove it into the table
        auto catalog_fk = new catalog::ForeignKey(
            sink_table->GetOid(), sink_col_ids, source_col_ids,
            fk.upd_action, fk.del_action, fk.constraint_name);
        source_table->AddForeignKey(catalog_fk);

        // Register FK with the sink table for delete/update actions
        sink_table->RegisterForeignKeySource(table_name);

        // Add a non-unique index on the source table if needed
        if (catalog_fk->GetUpdateAction() != FKConstrActionType::NOACTION ||
            catalog_fk->GetDeleteAction() != FKConstrActionType::NOACTION) {
          std::vector<std::string> source_col_names =
              fk.foreign_key_sources;
          std::string index_name =
              source_table->GetName() + "_FK_" + std::to_string(count);
          catalog->CreateIndex(database_name, source_table->GetName(),
                               source_col_ids, index_name, false,
                               IndexType::BWTREE, txn);
          count++;

#ifdef LOG_DEBUG_ENABLED
          LOG_DEBUG("Added a FOREIGN index on in %s.", table_name.c_str());
          LOG_DEBUG("Foreign key column names: ");
          for (auto c : source_col_names) {
            LOG_DEBUG("FK col name: %s", c.c_str());
          }
#endif
        }
      }
    }
  } else if (txn->GetResult() == ResultType::FAILURE) {
    LOG_TRACE("Creating table failed!");
    return (false);
  } else {
    LOG_TRACE("Result is: %s",
              ResultTypeToString(txn->GetResult()).c_str());
  }
  return (true);
}

bool CreateExecutor::CreateIndex(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  std::string table_name = node.GetTableName();
  std::string index_name = node.GetIndexName();
  bool unique_flag = node.IsUnique();
  IndexType index_type = node.GetIndexType();

  auto key_attrs = node.GetKeyAttrs();

  ResultType result = catalog::Catalog::GetInstance()->CreateIndex(
      DEFAULT_DB_NAME, table_name, key_attrs, index_name, unique_flag,
      index_type, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Creating table succeeded!");
  } else if (txn->GetResult() == ResultType::FAILURE) {
    LOG_TRACE("Creating table failed!");
  } else {
    LOG_TRACE("Result is: %s",
              ResultTypeToString(txn->GetResult()).c_str());
  }
  return (true);
}

bool CreateExecutor::CreateTrigger(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  std::string database_name = node.GetDatabaseName();
  std::string table_name = node.GetTableName();
  std::string trigger_name = node.GetTriggerName();

  trigger::Trigger newTrigger(node);

  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      database_name, table_name, txn);

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

  catalog::TriggerCatalog::GetInstance().InsertTrigger(
      table_object->GetTableOid(), trigger_name,
      newTrigger.GetTriggerType(), newTrigger.GetFuncname(),
      newTrigger.GetArgs(), when, time_stamp, pool_.get(), txn);
  // ask target table to update its trigger list variable
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          database_name, table_name, txn);
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

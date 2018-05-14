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
#include "common/exception.h"
#include "catalog/catalog.h"
#include "catalog/foreign_key.h"
#include "catalog/system_catalogs.h"
#include "catalog/sequence_catalog.h"
#include "concurrency/transaction_context.h"
#include "executor/executor_context.h"
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

    // if query was for creating sequence
    case CreateType::SEQUENCE: {
      result = CreateSequence(node);
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
      catalog::Catalog::GetInstance()->CreateDatabase(database_name, txn);
  txn->SetResult(result);
  LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  return (true);
}

bool CreateExecutor::CreateSchema(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  auto database_name = node.GetDatabaseName();
  auto schema_name = node.GetSchemaName();
  // invoke logic within catalog.cpp
  ResultType result = catalog::Catalog::GetInstance()->CreateSchema(
      database_name, schema_name, txn);
  txn->SetResult(result);
  LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  return (true);
}

bool CreateExecutor::CreateTable(const planner::CreatePlan &node) {
  auto current_txn = context_->GetTransaction();
  std::string table_name = node.GetTableName();
  std::string schema_name = node.GetSchemaName();
  std::string database_name = node.GetDatabaseName();
  std::string session_namespace = node.GetSessionNamespace();
  std::unique_ptr<catalog::Schema> schema(node.GetSchema());

  //check foreign key schema first
  CheckForeignKeySchema(schema_name, database_name, session_namespace, node, current_txn);

  ResultType result = catalog::Catalog::GetInstance()->CreateTable(
      database_name, schema_name, table_name, std::move(schema), current_txn);
  current_txn->SetResult(result);
  current_txn->SetCommitOption(node.GetCommitOption());
  if (current_txn->GetResult() == ResultType::SUCCESS) {
    //if created a temp table.
    if (schema_name.find(TEMP_NAMESPACE_PREFIX) != std::string::npos) {
      auto catalog = catalog::Catalog::GetInstance();
      //get the table object
      auto table_object = catalog->GetTableObject(database_name, schema_name, session_namespace,
                                                  table_name, current_txn);
      //record the table oid if we need to delete rows or drop the temp table.
      if (node.GetCommitOption() == ONCOMMIT_DROP || node.GetCommitOption() == ONCOMMIT_DELETE_ROWS) {
          current_txn->AddTempTableObject(table_object);
      }
    }
    LOG_TRACE("Creating table succeeded!");

    // Add the foreign key constraint (or other multi-column constraints)
    if (node.GetForeignKeys().empty() == false) {
      int count = 1;
      auto catalog = catalog::Catalog::GetInstance();
      auto source_table = catalog->GetTableWithName(database_name, schema_name, session_namespace,
                                                    table_name, current_txn);

      for (auto fk : node.GetForeignKeys()) {
        auto sink_table = catalog->GetTableWithName(
            database_name, fk.sink_table_schema, session_namespace, fk.sink_table_name, current_txn);
        // Source Column Offsets
        std::vector<oid_t> source_col_ids;
        for (auto col_name : fk.foreign_key_sources) {
          oid_t col_id = source_table->GetSchema()->GetColumnID(col_name);
          if (col_id == INVALID_OID) {
            std::string error = StringUtil::Format(
                "Invalid source column name '%s.%s' for foreign key '%s'",
                table_name.c_str(), col_name.c_str(),
                fk.constraint_name.c_str());
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
        auto catalog_fk = new catalog::ForeignKey(
            INVALID_OID, sink_table->GetOid(), sink_col_ids, source_col_ids,
            fk.upd_action, fk.del_action, fk.constraint_name);
        source_table->AddForeignKey(catalog_fk);

        // Register FK with the sink table for delete/update actions
        catalog_fk = new catalog::ForeignKey(
            source_table->GetOid(), INVALID_OID, sink_col_ids, source_col_ids,
            fk.upd_action, fk.del_action, fk.constraint_name);
        sink_table->RegisterForeignKeySource(catalog_fk);

        // Add a non-unique index on the source table if needed
        std::vector<std::string> source_col_names = fk.foreign_key_sources;
        std::string index_name = table_name + "_FK_" + sink_table->GetName() +
                                 "_" + std::to_string(count);
        catalog->CreateIndex(database_name, schema_name, session_namespace, table_name,
                             source_col_ids, index_name, false,
                             IndexType::BWTREE, current_txn);
        count++;

#ifdef LOG_DEBUG_ENABLED
        LOG_DEBUG("Added a FOREIGN index on in %s.\n", table_name.c_str());
        LOG_DEBUG("Foreign key column names: \n");
        for (auto c : source_col_names) {
          LOG_DEBUG("FK col name: %s\n", c.c_str());
        }
        for (auto c : fk.foreign_key_sinks) {
          LOG_DEBUG("FK sink col name: %s\n", c.c_str());
        }
#endif
      }
    }
  } else if (current_txn->GetResult() == ResultType::FAILURE) {
    LOG_TRACE("Creating table failed!");
  } else {
    LOG_TRACE("Result is: %s",
              ResultTypeToString(current_txn->GetResult()).c_str());
  }

  return (true);
}

//check whether the foriegn key schema and the current schema are not only one in temp.
void CreateExecutor::CheckForeignKeySchema(
  const std::string &schema_name, const std::string &database_name,
  const std::string &session_namespace, const planner::CreatePlan &node,
  concurrency::TransactionContext *txn) {
  for (auto fk : node.GetForeignKeys()) {
      auto catalog = catalog::Catalog::GetInstance();
      //get table obejct for use
      auto sink_table_object = catalog->GetTableObject(database_name, fk.sink_table_schema,
                                                  session_namespace, fk.sink_table_name, txn);
      std::string sink_table_schema = sink_table_object->GetSchemaName();
      //if target is under temp but current not.
      if (schema_name.find(TEMP_NAMESPACE_PREFIX) == std::string::npos) {
          if (sink_table_schema.find(TEMP_NAMESPACE_PREFIX) != std::string::npos) {
             throw ConstraintException("ERROR: constraints on permanent tables may reference only permanent tables");
          }
      }

      //if current is temp, but target is not
      if (schema_name.find(TEMP_NAMESPACE_PREFIX) != std::string::npos) {
        if (sink_table_schema.find(TEMP_NAMESPACE_PREFIX) == std::string::npos) {
            throw ConstraintException("ERROR: constraints on temporary tables may reference only temporary tables");
        }
    }
  }
}

bool CreateExecutor::CreateIndex(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  std::string database_name = node.GetDatabaseName();
  std::string schema_name = node.GetSchemaName();
  std::string table_name = node.GetTableName();
  std::string index_name = node.GetIndexName();
  std::string session_namespace = node.GetSessionNamespace();
  bool unique_flag = node.IsUnique();
  IndexType index_type = node.GetIndexType();

  auto key_attrs = node.GetKeyAttrs();

  ResultType result = catalog::Catalog::GetInstance()->CreateIndex(
      database_name, schema_name, session_namespace,
      table_name, key_attrs, index_name,
      unique_flag, index_type, txn);
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
  std::string session_namespace = node.GetSessionNamespace();

  trigger::Trigger newTrigger(node);
  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      database_name, schema_name, session_namespace, table_name, txn);

  // durable trigger: insert the information of this trigger in the trigger
  // catalog table
  auto time_stamp = type::ValueFactory::GetTimestampValue(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  CopySerializeOutput output;
  newTrigger.SerializeWhen(output, table_object->GetDatabaseOid(),
                           table_object->GetTableOid(), txn);
  auto when = type::ValueFactory::GetVarbinaryValue(
      (const unsigned char *)output.Data(), (int32_t)output.Size(), true);

  catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(table_object->GetDatabaseOid())
      ->GetTriggerCatalog()
      ->InsertTrigger(table_object->GetTableOid(), trigger_name,
                      newTrigger.GetTriggerType(), newTrigger.GetFuncname(),
                      newTrigger.GetArgs(), when, time_stamp, pool_.get(), txn);
  // ask target table to update its trigger list variable
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          database_name, schema_name, session_namespace, table_name, txn);
  target_table->UpdateTriggerListFromCatalog(txn);

  // hardcode SUCCESS result for txn
  txn->SetResult(ResultType::SUCCESS);

  // FIXME: We are missing failure logic here!
  // We seem to be assuming that we will always succeed
  // in installing the trigger.

  return (true);
}

bool CreateExecutor::CreateSequence(const planner::CreatePlan &node) {
  auto txn = context_->GetTransaction();
  std::string database_name = node.GetDatabaseName();
  std::string sequence_name = node.GetSequenceName();

  auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
      database_name, txn);

  catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(database_object->GetDatabaseOid())
      ->GetSequenceCatalog()
      ->InsertSequence(
      database_object->GetDatabaseOid(), sequence_name,
      node.GetSequenceIncrement(), node.GetSequenceMaxValue(),
      node.GetSequenceMinValue(), node.GetSequenceStart(),
      node.GetSequenceCycle(), pool_.get(), txn);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_DEBUG("Creating sequence succeeded!");
  } else if (txn->GetResult() == ResultType::FAILURE) {
    LOG_DEBUG("Creating sequence failed!");
  } else {
    LOG_DEBUG("Result is: %s",
              ResultTypeToString(txn->GetResult()).c_str());
  }

  // Notice this action will always return true, since any exception
  // will be handled in CreateSequence function in SequenceCatalog.
  return (true);
}


}  // namespace executor
}  // namespace peloton

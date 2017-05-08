//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metrics_catalog.cpp
//
// Identification: src/catalog/query_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/trigger_catalog.h"
#include "common/macros.h"
#include "common/logger.h"

namespace peloton {
namespace catalog {

TriggerCatalog *TriggerCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static std::unique_ptr<TriggerCatalog> trigger_catalog(
      new TriggerCatalog(txn));

  return trigger_catalog.get();
}

TriggerCatalog::TriggerCatalog(concurrency::Transaction *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." TRIGGER_CATALOG_NAME
                      " ("
                      "trigger_name          VARCHAR NOT NULL, "
                      "database_oid          INT NOT NULL, "
                      "table_oid             INT NOT NULL, "
                      "trigger_type          INT NOT NULL, "
                      "fire_condition        VARCHAR, "
                      "function_name         VARCHAR, "
                      "function_arguments    VARCHAR, "
                      "time_stamp            INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, TRIGGER_CATALOG_NAME,
      {"database_oid", "table_oid", "trigger_type"}, TRIGGER_CATALOG_NAME "_skey0",
      false, IndexType::BWTREE, txn);

  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, TRIGGER_CATALOG_NAME,
      {"database_oid", "table_oid"}, TRIGGER_CATALOG_NAME "_skey1",
      false, IndexType::BWTREE, txn);
}

TriggerCatalog::~TriggerCatalog() {}

bool TriggerCatalog::InsertTrigger(
    std::string trigger_name, oid_t database_oid, oid_t table_oid,
    int16_t trigger_type,
    std::string fire_condition, //TODO: this actually should be expression
    std::string function_name,
    std::string function_arguments,
    int64_t time_stamp, type::AbstractPool *pool,
    concurrency::Transaction *txn) {


  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));


  LOG_INFO("type of trigger inserted:%d", trigger_type);

  auto val0 = type::ValueFactory::GetVarcharValue(trigger_name, pool);
  auto val1 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val3 = type::ValueFactory::GetIntegerValue(trigger_type);
  auto val4 = type::ValueFactory::GetVarcharValue(fire_condition, pool);
  auto val5 = type::ValueFactory::GetVarcharValue(function_name, pool);
  auto val6 = type::ValueFactory::GetVarcharValue(function_arguments, pool);
  auto val7 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(ColumnId::TRIGGER_NAME, val0, pool);
  tuple->SetValue(ColumnId::DATABASE_OID, val1, pool);
  tuple->SetValue(ColumnId::TABLE_OID, val2, pool);
  tuple->SetValue(ColumnId::TRIGGER_TYPE, val3, pool);
  tuple->SetValue(ColumnId::FIRE_CONDITION, val4, pool);
  tuple->SetValue(ColumnId::FUNCTION_NAME, val5, pool);
  tuple->SetValue(ColumnId::FUNCTION_ARGS, val6, pool);
  tuple->SetValue(ColumnId::TIME_STAMP, val7, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

ResultType TriggerCatalog::DropTrigger(const std::string &database_name,
                        const std::string &table_name,
                       const std::string &trigger_name,
                       concurrency::Transaction *txn) {

  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to drop trigger: %s", table_name.c_str());
    return ResultType::FAILURE;
  }

  // Checking if statement is valid
  oid_t database_oid =
    DatabaseCatalog::GetInstance()->GetDatabaseOid(database_name, txn);
  if (database_oid == INVALID_OID) {
    LOG_TRACE("Cannot find database  %s!", database_name.c_str());
    return ResultType::FAILURE;
  }

  oid_t table_oid =
    TableCatalog::GetInstance()->GetTableOid(table_name, txn);
  if (table_oid == INVALID_OID) {
    LOG_TRACE("Cannot find table %s!", table_name.c_str());
    return ResultType::FAILURE;
  }

  oid_t trigger_oid =
    TriggerCatalog::GetInstance()->GetTriggerOid(trigger_name, table_oid, database_oid, txn);
  if (trigger_oid == INVALID_OID) {
    LOG_TRACE("Cannot find trigger %s to drop!", trigger_name.c_str());
    return ResultType::FAILURE;
  }

  ResultType result = DeleteTrigger(trigger_oid, txn);

  return result;
}

bool TriggerCatalog::DeleteTrigger(oid_t trigger_oid, concurrency::Transaction *txn) {
  oid_t index_offset = 0;  // Index of trigger_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(trigger_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

commands::TriggerList* TriggerCatalog::GetTriggersByType(oid_t database_oid,
                                          oid_t table_oid,
                                          int16_t trigger_type,
                                          concurrency::Transaction *txn) {
  LOG_INFO("get triggers for table %d", table_oid);
  LOG_INFO("want type %d", trigger_type);
  // select trigger_name, fire condition, function_name, function_args
  std::vector<oid_t> column_ids({ColumnId::TRIGGER_NAME, ColumnId::FIRE_CONDITION, ColumnId::FUNCTION_NAME, ColumnId::FUNCTION_ARGS});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;          // Secondary key index
  std::vector<type::Value> values;
  // where database_oid = args.database_oid and table_oid = args.table_oid and trigger_type = args.trigger_type
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(trigger_type).Copy());

  // the result is a vector of executor::LogicalTile
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  // carefull! the result tile could be null!
  if (result_tiles == nullptr) {
    LOG_INFO("no trigger on table %d", table_oid);
  } else {
    LOG_INFO("size of the result tiles = %lu", result_tiles->size());
  }

  // create the trigger list
  commands::TriggerList* newTriggerList = new commands::TriggerList();
  if (result_tiles != nullptr) {
    for (unsigned int i = 0; i < result_tiles->size(); i++) {
      size_t tuple_count = (*result_tiles)[i]->GetTupleCount();
      for (size_t j = 0; j < tuple_count; j++) {
        LOG_INFO("trigger_name is %s", (*result_tiles)[i]->GetValue(j, 0).ToString().c_str());
        LOG_INFO("FIRE_CONDITION is %s", (*result_tiles)[i]->GetValue(j, 1).ToString().c_str());
        LOG_INFO("FUNCTION_NAME is %s", (*result_tiles)[i]->GetValue(j, 2).ToString().c_str());
        LOG_INFO("FUNCTION_ARGS is %s", (*result_tiles)[i]->GetValue(j, 3).ToString().c_str());
        LOG_INFO("reach here safely");
        // create a new trigger instance
        commands::Trigger newTrigger((*result_tiles)[i]->GetValue(j, 0).ToString(),
                                     trigger_type,
                                     (*result_tiles)[i]->GetValue(j, 2).ToString(),
                                     (*result_tiles)[i]->GetValue(j, 3).ToString(),
                                     (*result_tiles)[i]->GetValue(j, 1).ToString());
        commands::TriggerList tmpTriggerList;
        tmpTriggerList.AddTrigger(newTrigger);

        LOG_INFO("reach here safely 2");
        if (newTriggerList == nullptr) {
          LOG_INFO("newTriggerList is null....");
        }
        newTriggerList->AddTrigger(newTrigger);
        LOG_INFO("reach here safely 3");
      }
    }
  }
  LOG_INFO("reach here safely 4");
  return newTriggerList;
}

commands::TriggerList* TriggerCatalog::GetTriggers(oid_t database_oid,
                                          oid_t table_oid,
                                          concurrency::Transaction *txn) {
  LOG_INFO("get triggers for table %d", table_oid);
  // select trigger_name, fire condition, function_name, function_args
  std::vector<oid_t> column_ids({ColumnId::TRIGGER_NAME, ColumnId::TRIGGER_TYPE, ColumnId::FIRE_CONDITION, ColumnId::FUNCTION_NAME, ColumnId::FUNCTION_ARGS});
  oid_t index_offset = IndexId::DATABASE_TABLE_KEY_1; // multi-column (database_oid, table_oid) bwtree index
  std::vector<type::Value> values;
  // where database_oid = args.database_oid and table_oid = args.table_oid and trigger_type = args.trigger_type
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // the result is a vector of executor::LogicalTile
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  // carefull! the result tile could be null!
  if (result_tiles == nullptr) {
    LOG_INFO("no trigger on table %d", table_oid);
  } else {
    LOG_INFO("size of the result tiles = %lu", result_tiles->size());
  }

  // create the trigger list
  commands::TriggerList* newTriggerList = new commands::TriggerList();
  if (result_tiles != nullptr) {
    for (unsigned int i = 0; i < result_tiles->size(); i++) {
      size_t tuple_count = (*result_tiles)[i]->GetTupleCount();
      for (size_t j = 0; j < tuple_count; j++) {
        LOG_INFO("trigger_name is %s", (*result_tiles)[i]->GetValue(j, 0).ToString().c_str());
        LOG_INFO("FIRE_CONDITION is %s", (*result_tiles)[i]->GetValue(j, 2).ToString().c_str());
        LOG_INFO("FUNCTION_NAME is %s", (*result_tiles)[i]->GetValue(j, 3).ToString().c_str());
        LOG_INFO("FUNCTION_ARGS is %s", (*result_tiles)[i]->GetValue(j, 4).ToString().c_str());
        LOG_INFO("trigger_type is %d", (*result_tiles)[i]->GetValue(j, 1).GetAs<int16_t>());

        // create a new trigger instance
        commands::Trigger newTrigger((*result_tiles)[i]->GetValue(j, 0).ToString(),
                                     (*result_tiles)[i]->GetValue(j, 1).GetAs<int16_t>(),
                                     (*result_tiles)[i]->GetValue(j, 3).ToString(),
                                     (*result_tiles)[i]->GetValue(j, 4).ToString(),
                                     (*result_tiles)[i]->GetValue(j, 2).ToString());

        commands::TriggerList tmpTriggerList;
        LOG_INFO("tmp trigger list size = %d", tmpTriggerList.GetTriggerListSize());
        LOG_INFO("new trigger list size = %d", newTriggerList->GetTriggerListSize());
        LOG_INFO("new trigger name = %s", newTrigger.GetTriggerName().c_str());
        LOG_INFO("new trigger type = %d", newTrigger.GetTriggerType());

        newTriggerList->AddTrigger(newTrigger);

      }
    }
  }

  return newTriggerList;
}

}  // End catalog namespace
}  // End peloton namespace

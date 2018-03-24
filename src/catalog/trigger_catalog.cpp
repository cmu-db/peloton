//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// trigger_catalog.cpp
//
// Identification: src/catalog/trigger_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"
#include "codegen/buffering_consumer.h"
#include "catalog/trigger_catalog.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

TriggerCatalog::TriggerCatalog(const std::string &database_name,
                               concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " + database_name +
                          "." CATALOG_SCHEMA_NAME "." TRIGGER_CATALOG_NAME
                          " ("
                          "oid          INT NOT NULL PRIMARY KEY, "
                          "tgrelid      INT NOT NULL, "
                          "tgname       VARCHAR NOT NULL, "
                          "tgfoid       VARCHAR, "
                          "tgtype       INT NOT NULL, "
                          "tgargs       VARCHAR, "
                          "tgqual       VARBINARY, "
                          "timestamp    TIMESTAMP NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
  Catalog::GetInstance()->CreateIndex(
      database_name, CATALOG_SCHEMA_NAME, TRIGGER_CATALOG_NAME,
      {ColumnId::TABLE_OID, ColumnId::TRIGGER_TYPE},
      TRIGGER_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);

  Catalog::GetInstance()->CreateIndex(
      database_name, CATALOG_SCHEMA_NAME, TRIGGER_CATALOG_NAME,
      {ColumnId::TABLE_OID}, TRIGGER_CATALOG_NAME "_skey1", false,
      IndexType::BWTREE, txn);

  Catalog::GetInstance()->CreateIndex(
      database_name, CATALOG_SCHEMA_NAME, TRIGGER_CATALOG_NAME,
      {ColumnId::TRIGGER_NAME, ColumnId::TABLE_OID},
      TRIGGER_CATALOG_NAME "_skey2", false, IndexType::BWTREE, txn);
}

TriggerCatalog::~TriggerCatalog() {}

bool TriggerCatalog::InsertTrigger(oid_t table_oid, std::string trigger_name,
                                   int16_t trigger_type, std::string proc_name,
                                   std::string function_arguments,
                                   type::Value fire_condition,
                                   type::Value timestamp,
                                   type::AbstractPool *pool,
                                   concurrency::TransactionContext *txn) {

  LOG_INFO("type of trigger inserted:%d", trigger_type);

  (void) pool;
  std::vector<std::vector<ExpressionPtr>> tuples;
  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];

  auto val0 = type::ValueFactory::GetIntegerValue(GetNextOid());
  auto val1 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val2 = type::ValueFactory::GetVarcharValue(trigger_name);
  auto val3 = type::ValueFactory::GetVarcharValue(proc_name);
  auto val4 = type::ValueFactory::GetIntegerValue(trigger_type);
  auto val5 = type::ValueFactory::GetVarcharValue(function_arguments);
  auto val6 = fire_condition;
  auto val7 = timestamp;

  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val0)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val1)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val2)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val3)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val4)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val5)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val6)));
  values.push_back(ExpressionPtr(new expression::ConstantValueExpression(
      val7)));

  // Insert the tuple
  return InsertTupleWithCompiledPlan(&tuples, txn);
}

ResultType TriggerCatalog::DropTrigger(const oid_t database_oid,
                                       const oid_t table_oid,
                                       const std::string &trigger_name,
                                       concurrency::TransactionContext *txn) {
  bool delete_success = DeleteTriggerByName(trigger_name, table_oid, txn);
  if (delete_success) {
    LOG_TRACE("Delete trigger successfully");
    // ask target table to update its trigger list variable
    storage::DataTable *target_table =
        storage::StorageManager::GetInstance()->GetTableWithOid(database_oid,
                                                                table_oid);
    target_table->UpdateTriggerListFromCatalog(txn);
    return ResultType::SUCCESS;
  }
  LOG_TRACE("Failed to delete trigger");
  return ResultType::FAILURE;
}

oid_t TriggerCatalog::GetTriggerOid(std::string trigger_name, oid_t table_oid,
                                    concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids({ColumnId::TRIGGER_OID});

  auto *name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                                    ColumnId::TRIGGER_NAME);
  name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                         catalog_table_->GetOid(), ColumnId::TRIGGER_NAME);

  expression::AbstractExpression *name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(trigger_name, nullptr).Copy());

  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr, name_const_expr);
  auto *oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::TABLE_OID);
  oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                        catalog_table_->GetOid(), ColumnId::TABLE_OID);

  expression::AbstractExpression *oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, oid_expr, oid_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, name_equality_expr,
          oid_equality_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  oid_t trigger_oid = INVALID_OID;
  if (result_tuples.size() == 0) {
    // LOG_INFO("trigger %s doesn't exist", trigger_name.c_str());
  } else {
    PELOTON_ASSERT(result_tuples.size() <= 1);
    if (result_tuples.size() != 0) {
      trigger_oid = result_tuples[0].GetValue(0).GetAs<oid_t>();
    }
  }

  return trigger_oid;
}

bool TriggerCatalog::DeleteTriggerByName(const std::string &trigger_name,
                                         oid_t table_oid,
                                         concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *trigger_name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                           ColumnId::TRIGGER_NAME);
  trigger_name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                               catalog_table_->GetOid(),
                               ColumnId::TRIGGER_NAME);
  expression::AbstractExpression *trigger_name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(trigger_name, nullptr).Copy());
  expression::AbstractExpression *trigger_name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, trigger_name_expr,
          trigger_name_const_expr);

  auto *table_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::TABLE_OID);
  table_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::TABLE_OID);

  expression::AbstractExpression *table_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *table_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, table_oid_expr, table_oid_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, trigger_name_equality_expr,
          table_oid_equality_expr);
  return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

std::unique_ptr<trigger::TriggerList> TriggerCatalog::GetTriggersByType(
    oid_t table_oid, int16_t trigger_type,
    concurrency::TransactionContext *txn) {
  LOG_INFO("Get triggers for table %d", table_oid);
  std::vector<oid_t> column_ids(all_column_ids);

  auto *type_expr =
      new expression::TupleValueExpression(type::TypeId::SMALLINT, 0,
                                                    ColumnId::TRIGGER_TYPE);
  type_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                         catalog_table_->GetOid(), ColumnId::TRIGGER_TYPE);

  expression::AbstractExpression *type_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetSmallIntValue(trigger_type).Copy());
  expression::AbstractExpression *type_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, type_expr, type_const_expr);

  auto *oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::TABLE_OID);
  oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                              catalog_table_->GetOid(), ColumnId::TABLE_OID);

  expression::AbstractExpression *oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, oid_expr, oid_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, type_equality_expr,
          oid_equality_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  // carefull! the result could be null!
  LOG_INFO("size of the result tiles = %lu", result_tuples.size());

  // create the trigger list
  std::unique_ptr<trigger::TriggerList> new_trigger_list{
      new trigger::TriggerList()};

  for (unsigned int i = 0; i < result_tuples.size(); i++) {
    // create a new trigger instance
    trigger::Trigger new_trigger(result_tuples[i].GetValue(ColumnId::TRIGGER_NAME).ToString(),
                                 trigger_type,
                                 result_tuples[i].GetValue(ColumnId::FUNCTION_NAME).ToString(),
                                 result_tuples[i].GetValue(ColumnId::FUNCTION_ARGS).ToString(),
                                 result_tuples[i].GetValue(ColumnId::FIRE_CONDITION).GetData());
    new_trigger_list->AddTrigger(new_trigger);
  }

  return new_trigger_list;
}

std::unique_ptr<trigger::TriggerList> TriggerCatalog::GetTriggers(
    oid_t table_oid, concurrency::TransactionContext *txn) {
  // LOG_DEBUG("Get triggers for table %d", table_oid);

  std::vector<oid_t> column_ids(all_column_ids);

  auto *oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::TABLE_OID);
  oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                              catalog_table_->GetOid(), ColumnId::TABLE_OID);

  expression::AbstractExpression *oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, oid_expr, oid_const_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, oid_equality_expr, txn);

  // carefull! the result tile could be null!

  // create the trigger list
  std::unique_ptr<trigger::TriggerList> new_trigger_list{
      new trigger::TriggerList()};

  for (unsigned int i = 0; i < result_tuples.size(); i++) {
    // create a new trigger instance
    trigger::Trigger new_trigger(result_tuples[i].GetValue(ColumnId::TRIGGER_NAME).ToString(),
                                 result_tuples[i].GetValue(ColumnId::TRIGGER_TYPE).GetAs<int16_t>(),
                                 result_tuples[i].GetValue(ColumnId::FUNCTION_NAME).ToString(),
                                 result_tuples[i].GetValue(ColumnId::FUNCTION_ARGS).ToString(),
                                 result_tuples[i].GetValue(ColumnId::FIRE_CONDITION).GetData());
    new_trigger_list->AddTrigger(new_trigger);
  }

  return new_trigger_list;
}

}  // namespace catalog
}  // namespace peloton

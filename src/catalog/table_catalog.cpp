//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_catalog.cpp
//
// Identification: src/catalog/table_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/table_catalog.h"

namespace peloton {
namespace catalog {

TableCatalog *TableCatalog::GetInstance(void) {
  static std::unique_ptr<TableCatalog> table_catalog(new TableCatalog());
  return table_catalog.get();
}

TableCatalog::TableCatalog()
    : AbstractCatalog(TABLE_CATALOG_OID, TABLE_CATALOG_NAME,
                      InitializeTableCatalogSchema().release()) {}

std::unique_ptr<catalog::Schema> TableCatalog::InitializeTableCatalogSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_id", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "table_name", true);
  table_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "database_id", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(
      type::Type::VARCHAR, max_name_size, "database_name", true);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_catalog_schema(
      new catalog::Schema({table_id_column, table_name_column,
                           database_id_column, database_name_column}));

  return table_catalog_schema;
}

bool TableCatalog::Insert(oid_t table_id, std::string table_name,
                          oid_t database_id, std::string database_name,
                          type::AbstractPool *pool,
                          concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val1 = type::ValueFactory::GetIntegerValue(table_id);
  auto val2 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(database_id);
  auto val4 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool TableCatalog::DeleteByOid(oid_t id, concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Delete node
  planner::DeletePlan delete_node(catalog_table_.get(), false);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Index scan as child node
  std::vector<oid_t> column_ids;  // No projection
  auto index = catalog_table_->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(id).Copy());

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  planner::IndexScanPlan index_scan_node(catalog_table_.get(), nullptr,
                                         column_ids, index_scan_desc);

  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Parent-Child relationship
  delete_node.AddChild(std::move(index_scan_node));
  delete_executor.AddChild(&index_scan_executor);
  delete_executor.Init();
  bool status = delete_executor.Execute();

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return status;
}

std::string TableCatalog::GetTableNameByOid(oid_t id,
                                            concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Index scan
  std::vector<oid_t> column_ids({1});  // table_name
  auto index = catalog_table_->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(id).Copy());

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  planner::IndexScanPlan index_scan_node(catalog_table_.get(), nullptr,
                                         column_ids, index_scan_desc);

  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Execute
  std::string table_name;
  index_scan_executor.Init();

  if (index_scan_executor.Execute()) {
    auto result = index_scan_executor.GetOutput();
    PL_ASSERT(result->GetTupleCount() <= 1);  // Oid is unique
    if (result->GetTupleCount() != 0) {
      table_name = result->GetValue(
          0, 0);  // After projection left 1 column
    }
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return table_name;
}

std::string TableCatalog::GetDatabaseNameByOid(oid_t id,
                                               concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Index scan
  std::vector<oid_t> column_ids({2});  // database_name
  auto index = catalog_table_->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(id).Copy());

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  planner::IndexScanPlan index_scan_node(catalog_table_.get(), nullptr,
                                         column_ids, index_scan_desc);

  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Execute
  std::string database_name;
  index_scan_executor.Init();

  if (index_scan_executor.Execute()) {
    auto result = index_scan_executor.GetOutput();
    PL_ASSERT(result->GetTupleCount() <= 1);  // Oid is unique
    if (result->GetTupleCount() != 0) {
      database_name = result->GetValue(
          0, 0);  // After projection left 1 column
    }
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return database_name;
}

oid_t TableCatalog::GetOidByName(std::string &table_name,
                                 std::string &database_name,
                                 concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Index scan
  std::vector<oid_t> column_ids({0});  // table_id
  auto index = catalog_table_->GetIndex(1);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(1);
  key_column_ids.push_back(2);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  planner::IndexScanPlan index_scan_node(catalog_table_.get(), nullptr,
                                         column_ids, index_scan_desc);

  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Execute
  oid_t oid = INVALID_OID;
  index_scan_executor.Init();

  if (index_scan_executor.Execute()) {
    auto result = index_scan_executor.GetOutput();
    PL_ASSERT(result->GetTupleCount() <=
              1);  // table_name + database_name is unique identifier
    if (result->GetTupleCount() != 0) {
      oid = result->GetValue(
          0, 0);  // After projection left 1 column
    }
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return oid;
}

}  // End catalog namespace
}  // End peloton namespace
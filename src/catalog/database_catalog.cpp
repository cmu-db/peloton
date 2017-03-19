//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.cpp
//
// Identification: src/catalog/database_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/database_catalog.h"

namespace peloton {
namespace catalog {

DatabaseCatalog *DatabaseCatalog::GetInstance(void) {
  static std::unique_ptr<DatabaseCatalog> database_catalog(
      new DatabaseCatalog());
  return database_catalog.get();
}

DatabaseCatalog::DatabaseCatalog() :
  AbstractCatalog(GetNextOid(), TABLE_CATALOG_NAME, InitializeTableCatalogSchema().release()) {}

std::unique_ptr<catalog::Schema> DatabaseCatalog::InitializeDatabaseCatalogSchema() {
  const std::string not_null_constraint_name = "not_null";
  const std::string primary_key_constraint_name = "primary_key";

  auto database_id_column = catalog::Column(type::Type::INTEGER,
                                   type::Type::GetTypeSize(type::Type::INTEGER),
                                   "database_id", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, primary_key_constraint_name));
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_name_column = catalog::Column(type::Type::VARCHAR, max_name_size,
                                     "database_name", true);
  database_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));
  return database_catalog_schema;
}

bool DatabaseCatalog::Insert(
    oid_t database_id, std::string database_name,
    type::AbstractPool *pool, concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));
  
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetVarcharValue(database_name, nullptr);
  
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  
  return InsertTuple(tuple, txn);
}


bool DatabaseCatalog::DeleteByOid(oid_t id, concurrency::Transaction *txn) {
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

std::string GetNameByOid(oid_t id, concurrency::Transaction *txn) {
  std::string database_name("");
  auto result = GetXByOid(id, 1, txn);  // Database name
  if (result->GetTupleCount() != 0) {
  database_name = result->GetValue(
      0, 0);  // After projection left 1 column
  }
  return name;
}

oid_t GetOidByName(std::string &name, concurrency::Transaction *txn) {
  oid_t oid = INVALID_OID;
  auto result = GetXByOid(name, 0, txn);  // Database name
  if (result->GetTupleCount() != 0) {
  oid = result->GetValue(
      0, 0);  // After projection left 1 column
  }
  return oid;
}

executor::LogicalTile *GetXByOid(oid_t id, int idx, concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Index scan
  std::vector<oid_t> column_ids({idx});  // idx
  auto index = catalog_table_->GetIndex(0); // by oid
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
  // std::string database_name("");
  index_scan_executor.Init();

  if (index_scan_executor.Execute()) {
    auto result = index_scan_executor.GetOutput();
    PL_ASSERT(result->GetTupleCount() <= 1);  // Oid is unique
    // if (result->GetTupleCount() != 0) {
    //   database_name = result->GetValue(
    //       0, 0);  // After projection left 1 column
    // }
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return result;
}

executor::LogicalTile *GetXByName(std::string &name, int idx, concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Index scan
  std::vector<oid_t> column_ids({idx});  // idx
  auto index = catalog_table_->GetIndex(1); // by name
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  planner::IndexScanPlan index_scan_node(catalog_table_.get(), nullptr,
                                         column_ids, index_scan_desc);

  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Execute
  // std::string database_name("");
  index_scan_executor.Init();

  if (index_scan_executor.Execute()) {
    auto result = index_scan_executor.GetOutput();
    PL_ASSERT(result->GetTupleCount() <= 1);  // Oid is unique
    // if (result->GetTupleCount() != 0) {
    //   database_name = result->GetValue(
    //       0, 0);  // After projection left 1 column
    // }
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return result;
}



}  // End catalog namespace
}  // End peloton namespace
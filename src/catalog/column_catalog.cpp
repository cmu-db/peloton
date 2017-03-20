//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.cpp
//
// Identification: src/catalog/column_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
// column_catalog table
// table_id--name--type--offset--isPrimar--hasConstrain
// table_id+name is primary key pair
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/column_catalog.h"

namespace peloton {
namespace catalog {

ColumnCatalog *ColumnCatalog::GetInstance(void) {
  static std::unique_ptr<ColumnCatalog> column_catalog(new ColumnCatalog());
  return column_catalog.get();
}

ColumnCatalog::ColumnCatalog()
    : AbstractCatalog(COLUMN_CATALOG_OID, COLUMN_CATALOG_NAME,
                      InitializeSchema().release()) {}

// column_catalog table
// table_id--name--type--offset--isPrimar--hasConstrain
// table_id+name is primary key pair
std::unique_ptr<catalog::Schema> ColumnCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "table_id", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_name_column =
      catalog::Column(type::Type::VARCHAR, max_name_size, "column_name", true);
  column_name_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  column_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_type_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "column_type", true);
  column_type_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_offset_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "column_offset", true);
  column_offset_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_isPrimary_column = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "column_isPrimary", true);
  column_isPrimary_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_hasConstrain_column = catalog::Column(
      type::Type::BOOLEAN, type::Type::GetTypeSize(type::Type::BOOLEAN),
      "column_hasConstrain", true);
  column_hasConstrain_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
      {table_id_column, column_name_column, column_type_column,
       column_offset_column, column_isPrimary_column,
       column_hasConstrain_column}));

  return column_catalog_schema;
}

// delete tuple with table_id(0) and column_name(1)
//===------------------------<
// --------------------------------------------===//
// ATTR 0 == table_id & ATTR 1 == name
//===--------------------------------------------------------------------===//
void DeleteByOid_Name(oid_t table_id, std::string &name,
                      concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  // Index scan as child node
  std::vector<oid_t> column_ids;  // No projection
  auto index = catalog_table_->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  key_column_ids.push_back(1);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(name).Copy(), nullptr);

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

// return column offset(3) with table_id(0) and column_name(1)
//===------------------------<
// --------------------------------------------===//
// ATTR 0 == table_id & ATTR 1 == name
//===--------------------------------------------------------------------===//
oid_t GetOffsetByOid_Name(oid_t table_id, std::string &name,
                          concurrency::Transaction *txn) {
  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({3});
  // column_catalog only support one primary key (table_id + name)
  auto index = catalog_table_->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;
  // only return one record
  std::unique_ptr<executor::LogicalTile> result_tile;

  key_column_ids.push_back(0);
  key_column_ids.push_back(1);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(name).Copy(), nullptr);

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan node(catalog_table_.get(), predicate, column_ids,
                              index_scan_desc);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Run the executor
  executor::IndexScanExecutor executor(&node, context.get());
  executor.Init();

  executor.Execute();
  result_tile = executor.GetOutput();

  PL_ASSERT(result_tile->GetTupleCount() <= 1);
  if (result_tile->GetTupleCount() != 0) {
    oid_t offset =
        result_tile->GetValue(0, 0);  // After projection left 1 column
  }

  return offset;
}

// return column type(2) with table_id(0) and column_name(1)
//===------------------------<
// --------------------------------------------===//
// ATTR 0 == table_id & ATTR 1 == name
//===--------------------------------------------------------------------===//
type::TypeId GetTypeByOid_Name(oid_t table_id, std::string &name,
                               concurrency::Transaction *txn) {
  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({2});
  // column_catalog only support one primary key (table_id + name)
  auto index = catalog_table_->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;
  // only return one record
  std::unique_ptr<executor::LogicalTile> result_tile;

  key_column_ids.push_back(0);
  key_column_ids.push_back(1);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(name).Copy());

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan node(catalog_table_.get(), predicate, column_ids,
                              index_scan_desc);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Run the executor
  executor::IndexScanExecutor executor(&node, context.get());
  executor.Init();

  executor.Execute();
  result_tile = executor.GetOutput();

  PL_ASSERT(result_tile->GetTupleCount() <= 1);
  if (result_tile->GetTupleCount() != 0) {
    auto type = result_tile->GetValue(0, 0);  // After projection left 1 column
  }

  return type;
}

bool ColumnCatalog::Insert(oid_t table_id, std::string column_name,
                           type::TypeId column_type, oid_t column_offset,
                           bool column_isPrimary, bool column_hasConstrain,
                           type::AbstractPool *pool,
                           concurrency::Transaction *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val1 = type::ValueFactory::GetIntegerValue(table_id);
  auto val2 = type::ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(column_type);
  auto val4 = type::ValueFactory::GetIntegerValue(column_offset);
  auto val5 = type::ValueFactory::GetBooleanValue(column_isPrimary);
  auto val6 = type::ValueFactory::GetBooleanValue(column_hasConstrain);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);
  tuple->SetValue(4, val5, pool);
  tuple->SetValue(5, val6, pool);
  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}
}  // End catalog namespace
}  // End peloton namespace

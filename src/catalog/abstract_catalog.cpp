//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog.cpp
//
// Identification: src/catalog/abstract_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

AbstractCatalog::AbstractCatalog(oid_t catalog_table_id,
                                 std::string catalog_table_name,
                                 catalog::Schema *catalog_table_schema) {
  // Create catalog_table_
  catalog_table_ =
      std::shared_ptr<storage::DataTable>(storage::TableFactory::GetDataTable(
          START_OID, catalog_table_id, catalog_table_schema, catalog_table_name,
          DEFAULT_TUPLES_PER_TILEGROUP, true, false, true));

  // Add catalog_table_ into pg_catalog database
  Catalog::pg_catalog->AddTable(catalog_table_.get());

  // Insert columns into pg_attribute, note that insertion does not require
  // indexes on pg_attribute
  auto pg_attribute = catalog_table_id == COLUMN_CATALOG_OID
                          ? this
                          : ColumnCatalog::GetInstance();
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    pg_attribute->Insert(catalog_table_id, column.GetName(), column.GetOffset(),
                         column.GetType(), true, column.GetConstraints(),
                         nullptr);
  }

  // Index construction and adding contents of pg_database, pg_table, pg_index
  // should leave to catalog's constructor
}

bool AbstractCatalog::InsertTuple(std::unique_ptr<storage::Tuple> tuple,
                                  concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::InsertPlan node(catalog_table_.get(), std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  executor.Init();
  bool status = executor.Execute();

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
  return status;
}

// @brief   Delete a tuple using index scan
// @param   index_offset  Offset of index for scan
// @param   values        Values for search
// @param   txn           Transaction
// @return  Whether deletion is success
bool AbstractCatalog::DeleteWithIndexScan(oid_t index_offset,
                                          std::vector<type::Value> values,
                                          concurrency::Transaction *txn) {
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
  auto index = catalog_table_->GetIndex(index_offset);
  std::vector<oid_t> key_column_ids =
      index->GetMetadata()->GetKeySchema()->GetIndexedColumns();
  PL_ASSERT(values.size() == key_column_ids.size());
  std::vector<ExpressionType> expr_types(values.size(),
                                         ExpressionType::COMPARE_EQUAL);
  std::vector<expression::AbstractExpression *> runtime_keys;

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

// @brief   Index scan helper function
// @param   column_ids    Column ids for search (projection)
// @param   index_offset  Offset of index for scan
// @param   values        Values for search
// @param   txn           Transaction
// @return  Result of executor
executor::LogicalTile *AbstractCatalog::GetWithIndexScan(
    std::vector<oid_t> column_ids, oid_t index_offset,
    std::vector<type::Value> values, concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  // Index scan
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  auto index = catalog_table_->GetIndex(index_offset);
  std::vector<oid_t> key_column_ids =
      index->GetMetadata()->GetKeySchema()->GetIndexedColumns();
  PL_ASSERT(values.size() == key_column_ids.size());
  std::vector<ExpressionType> expr_types(values.size(),
                                         ExpressionType::COMPARE_EQUAL);
  std::vector<expression::AbstractExpression *> runtime_keys;

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);

  planner::IndexScanPlan index_scan_node(catalog_table_.get(), nullptr,
                                         column_ids, index_scan_desc);

  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Execute
  index_scan_executor.Init();
  index_scan_executor.Execute();
  auto result = index_scan_executor.GetOutput();

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return result;
}

}  // End catalog namespace
}  // End peloton namespace
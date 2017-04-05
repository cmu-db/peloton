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

AbstractCatalog::AbstractCatalog(oid_t catalog_table_oid,
                                 std::string catalog_table_name,
                                 catalog::Schema *catalog_table_schema,
                                 storage::Database *pg_catalog) {
  // Create catalog_table_
  catalog_table_ = storage::TableFactory::GetDataTable(
      CATALOG_DATABASE_OID, catalog_table_oid, catalog_table_schema,
      catalog_table_name, DEFAULT_TUPLES_PER_TILEGROUP, true, false, true);

  // Add catalog_table_ into pg_catalog database
  pg_catalog->AddTable(catalog_table_, true);
}

/*@brief   insert tuple(reord) helper function
* @param   tuple     tuple to be inserted
* @param   txn       Transaction
* @return  Whether insertion is Successful
*/
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
  planner::InsertPlan node(catalog_table_, std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  executor.Init();
  bool status = executor.Execute();

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
  return status;
}

/*@brief   Delete a tuple using index scan
* @param   index_offset  Offset of index for scan
* @param   values        Values for search
* @param   txn           Transaction
* @return  Whether deletion is Successful
*/
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
  planner::DeletePlan delete_node(catalog_table_, false);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Index scan as child node
  std::vector<oid_t> column_offsets;  // No projection
  auto index = catalog_table_->GetIndex(index_offset);
  std::vector<oid_t> key_column_offsets =
      index->GetMetadata()->GetKeySchema()->GetIndexedColumns();
  PL_ASSERT(values.size() == key_column_offsets.size());
  std::vector<ExpressionType> expr_types(values.size(),
                                         ExpressionType::COMPARE_EQUAL);
  std::vector<expression::AbstractExpression *> runtime_keys;

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_offsets, expr_types, values, runtime_keys);

  std::unique_ptr<planner::IndexScanPlan> index_scan_node(
      new planner::IndexScanPlan(catalog_table_, nullptr, column_offsets,
                                 index_scan_desc));

  executor::IndexScanExecutor index_scan_executor(index_scan_node.get(),
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

/*@brief   Index scan helper function
* @param   column_offsets    Column ids for search (projection)
* @param   index_offset      Offset of index for scan
* @param   values            Values for search
* @param   txn               Transaction
* @return  Unique pointer of vector of logical tiles
*/
std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
AbstractCatalog::GetResultWithIndexScan(std::vector<oid_t> column_offsets,
                                        oid_t index_offset,
                                        std::vector<type::Value> values,
                                        concurrency::Transaction *txn) {
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
  std::vector<oid_t> key_column_offsets =
      index->GetMetadata()->GetKeySchema()->GetIndexedColumns();
  PL_ASSERT(values.size() == key_column_offsets.size());
  std::vector<ExpressionType> expr_types(values.size(),
                                         ExpressionType::COMPARE_EQUAL);
  std::vector<expression::AbstractExpression *> runtime_keys;

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_offsets, expr_types, values, runtime_keys);

  planner::IndexScanPlan index_scan_node(catalog_table_, nullptr,
                                         column_offsets, index_scan_desc);

  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Execute
  PL_ASSERT(index_scan_executor.Init());
  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
      result_tiles(new std::vector<std::unique_ptr<executor::LogicalTile>>());

  while (index_scan_executor.Execute()) {
    result_tiles->push_back(std::unique_ptr<executor::LogicalTile>(
        index_scan_executor.GetOutput()));
  }

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }

  return result_tiles;
}

/*@brief   Add index on catalog table
* @param   key_attrs    indexed column offset(position)
* @param   index_oid    index id(global unique)
* @param   index_name   index name(global unique)
* @param   index_constraint     index constraints
* @return  Unique pointer of vector of logical tiles
*/
void AbstractCatalog::AddIndex(const std::vector<oid_t> &key_attrs,
                               oid_t index_oid, const std::string &index_name,
                               IndexConstraintType index_constraint) {
  auto schema = catalog_table_->GetSchema();
  auto key_schema = catalog::Schema::CopySchema(schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique_keys = (index_constraint == IndexConstraintType::PRIMARY_KEY) ||
                     (index_constraint == IndexConstraintType::UNIQUE);

  auto index_metadata = new index::IndexMetadata(
      index_name, index_oid, catalog_table_->GetOid(), CATALOG_DATABASE_OID,
      IndexType::BWTREE, index_constraint, schema, key_schema, key_attrs,
      unique_keys);

  std::shared_ptr<index::Index> key_index(
      index::IndexFactory::GetIndex(index_metadata));
  catalog_table_->AddIndex(key_index);

  // insert index record into index_catalog(pg_index) table
  // IndexCatalog::GetInstance()->InsertIndex(
  //     index_oid, index_name, COLUMN_CATALOG_OID, IndexType::BWTREE,
  //     index_constraint, unique_keys, pool_.get(), nullptr);

  LOG_TRACE("Successfully created primary key index '%s' for table '%s'",
            index_name.c_str(), COLUMN_CATALOG_NAME);
}

}  // End catalog namespace
}  // End peloton namespace

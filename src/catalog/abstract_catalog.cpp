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

#include "common/statement.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"

#include "index/index_factory.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"

#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"

#include "executor/delete_executor.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"
#include "executor/seq_scan_executor.h"

#include "storage/database.h"
#include "storage/storage_manager.h"
#include "storage/table_factory.h"

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

AbstractCatalog::AbstractCatalog(const std::string &catalog_table_ddl,
                                 concurrency::TransactionContext *txn) {
  // get catalog table schema
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto create_plan = std::dynamic_pointer_cast<planner::CreatePlan>(
      optimizer::Optimizer().BuildPelotonPlanTree(
          peloton_parser.BuildParseTree(catalog_table_ddl),
          DATABASE_CATALOG_NAME, txn));
  auto catalog_table_schema = create_plan->GetSchema();
  auto catalog_table_name = create_plan->GetTableName();

  // create catalog table
  Catalog::GetInstance()->CreateTable(
      CATALOG_DATABASE_NAME, catalog_table_name,
      std::unique_ptr<catalog::Schema>(catalog_table_schema), txn, true);

  // get catalog table oid
  auto catalog_table_object = Catalog::GetInstance()->GetTableObject(
      CATALOG_DATABASE_NAME, catalog_table_name, txn);

  // set catalog_table_
  try {
    catalog_table_ = storage::StorageManager::GetInstance()->GetTableWithOid(
        CATALOG_DATABASE_OID, catalog_table_object->GetTableOid());
  } catch (CatalogException &e) {
    LOG_TRACE("Can't find table %d! Return false",
              catalog_table_object->GetTableOid());
  }
}

/*@brief   insert tuple(reord) helper function
* @param   tuple     tuple to be inserted
* @param   txn       TransactionContext
* @return  Whether insertion is Successful
*/
bool AbstractCatalog::InsertTuple(std::unique_ptr<storage::Tuple> tuple,
                                  concurrency::TransactionContext *txn) {
  if (txn == nullptr)
    throw CatalogException("Insert tuple requires transaction");

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::InsertPlan node(catalog_table_, std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  executor.Init();
  bool status = executor.Execute();

  return status;
}

/*@brief   Delete a tuple using index scan
* @param   index_offset  Offset of index for scan
* @param   values        Values for search
* @param   txn           TransactionContext
* @return  Whether deletion is Successful
*/
bool AbstractCatalog::DeleteWithIndexScan(
    oid_t index_offset, std::vector<type::Value> values,
    concurrency::TransactionContext *txn) {
  if (txn == nullptr)
    throw CatalogException("Delete tuple requires transaction");

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Delete node
  planner::DeletePlan delete_node(catalog_table_);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Index scan as child node
  std::vector<oid_t> column_offsets;  // No projection
  auto index = catalog_table_->GetIndex(index_offset);
  PL_ASSERT(index != nullptr);
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

  return status;
}

/*@brief   Index scan helper function
* @param   column_offsets    Column ids for search (projection)
* @param   index_offset      Offset of index for scan
* @param   values            Values for search
* @param   txn               TransactionContext
* @return  Unique pointer of vector of logical tiles
*/
std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
AbstractCatalog::GetResultWithIndexScan(
    std::vector<oid_t> column_offsets, oid_t index_offset,
    std::vector<type::Value> values,
    concurrency::TransactionContext *txn) const {
  if (txn == nullptr) throw CatalogException("Scan table requires transaction");

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
  index_scan_executor.Init();
  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
      result_tiles(new std::vector<std::unique_ptr<executor::LogicalTile>>());

  while (index_scan_executor.Execute()) {
    result_tiles->push_back(std::unique_ptr<executor::LogicalTile>(
        index_scan_executor.GetOutput()));
  }

  return result_tiles;
}

/*@brief   Sequential scan helper function
* NOTE: try to use efficient index scan instead of sequential scan, but you
* shouldn't build too many indexes on one catalog table
* @param   column_offsets    Column ids for search (projection)
* @param   predicate         predicate for this sequential scan query
* @param   txn               TransactionContext
*
* @return  Unique pointer of vector of logical tiles
*/
std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
AbstractCatalog::GetResultWithSeqScan(std::vector<oid_t> column_offsets,
                                      expression::AbstractExpression *predicate,
                                      concurrency::TransactionContext *txn) {
  if (txn == nullptr) throw CatalogException("Scan table requires transaction");

  // Sequential scan
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  planner::SeqScanPlan seq_scan_node(catalog_table_, predicate, column_offsets);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  // Execute
  seq_scan_executor.Init();
  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
      result_tiles(new std::vector<std::unique_ptr<executor::LogicalTile>>());

  while (seq_scan_executor.Execute()) {
    result_tiles->push_back(
        std::unique_ptr<executor::LogicalTile>(seq_scan_executor.GetOutput()));
  }

  return result_tiles;
}

/*@brief   Add index on catalog table
* @param   key_attrs    indexed column offset(position)
* @param   index_oid    index id(global unique)
* @param   index_name   index name(global unique)
* @param   index_constraint     index constraints
* @return  Unique pointer of vector of logical tiles
* Note: Use catalog::Catalog::CreateIndex() if you can, only ColumnCatalog and
* IndexCatalog should need this
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

  // If you called AddIndex(), you need to insert the index record by yourself!
  // insert index record into index_catalog(pg_index) table
  // IndexCatalog::GetInstance()->InsertIndex(
  //     index_oid, index_name, COLUMN_CATALOG_OID, IndexType::BWTREE,
  //     index_constraint, unique_keys, pool_.get(), nullptr);

  LOG_TRACE("Successfully created index '%s' for table '%d'",
            index_name.c_str(), (int)catalog_table_->GetOid());
}

}  // namespace catalog
}  // namespace peloton

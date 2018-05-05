//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog.h
//
// Identification: src/include/catalog/abstract_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

#include "catalog/catalog_defaults.h"
#include "catalog/schema.h"
#include "codegen/query_cache.h"
#include "codegen/query.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace executor {
class LogicalTile;
}

namespace expression {
class AbstractExpression;
}

namespace codegen {
class WrappedTuple;
}

namespace storage {
class Database;
class DataTable;
class Tuple;
}  // namespace storage

namespace catalog {

using ExpressionPtr = std::unique_ptr<expression::AbstractExpression>;

class AbstractCatalog {
 public:
  virtual ~AbstractCatalog() {}

 protected:
  /* For pg_database, pg_table, pg_index, pg_column */
  AbstractCatalog(oid_t catalog_table_oid, std::string catalog_table_name,
                  catalog::Schema *catalog_table_schema,
                  storage::Database *pg_catalog);

  /* For other catalogs */
  AbstractCatalog(const std::string &catalog_table_ddl,
                  concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//

  /*@brief   insert tuple(reord) helper function
   * @param   tuple     tuple to be inserted
   * @param   txn       TransactionContext
   * @return  Whether insertion is Successful
   */
  bool InsertTuple(std::unique_ptr<storage::Tuple> tuple,
                   concurrency::TransactionContext *txn);

  /*@brief   insert tuple(reord) using compiled plan
  * @param   insert_values     tuples to be inserted
  * @param   txn       TransactionContext
  * @return  Whether insertion is Successful
  */
  bool InsertTupleWithCompiledPlan(const std::vector<std::vector<
      std::unique_ptr<expression::AbstractExpression>>> *insert_values,
                                     concurrency::TransactionContext *txn);

  /*@brief   Delete a tuple using index scan
   * @param   index_offset  Offset of index for scan
   * @param   values        Values for search
   * @param   txn           TransactionContext
   * @return  Whether deletion is Successful
   */
  bool DeleteWithIndexScan(oid_t index_offset, std::vector<type::Value> values,
                           concurrency::TransactionContext *txn);

  /*@brief   Delete a tuple using sequential scan
  * @param   column_offsets  Offset of seq scan
  * @param   predicate        Predicate used in the seq scan
  * @param   txn           TransactionContext
  * @return  Whether deletion is Successful
  */
  bool DeleteWithCompiledSeqScan(
      std::vector<oid_t> column_offsets,
      expression::AbstractExpression *predicate,
      concurrency::TransactionContext *txn);

  /*@brief   Index scan helper function
   * @param   column_offsets    Column ids for search (projection)
   * @param   index_offset      Offset of index for scan
   * @param   values            Values for search
   * @param   txn               TransactionContext
   * @return  Unique pointer of vector of logical tiles
   */
  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetResultWithIndexScan(std::vector<oid_t> column_offsets, oid_t index_offset,
                         std::vector<type::Value> values,
                         concurrency::TransactionContext *txn) const;

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
  GetResultWithSeqScan(std::vector<oid_t> column_offsets,
                       expression::AbstractExpression *predicate,
                       concurrency::TransactionContext *txn);

  /*@brief   Complied Sequential scan helper function
  * This is faster than the index scan as long as the query plan is cached
  * @param   column_offsets    Column ids for search (projection)
  * @param   predicate         predicate for this sequential scan query
  * @param   txn               TransactionContext
  *
  * @return  Unique pointer of vector of logical tiles
  */
  std::vector<codegen::WrappedTuple> GetResultWithCompiledSeqScan(
      std::vector<oid_t> column_offsets,
      expression::AbstractExpression *predicate,
      concurrency::TransactionContext *txn) const;

  /*@brief   Update specific columns using compiled sequential scan
   * @param   update_columns    Columns to be updated
   * @param   update_values     Values to be updated
   * @param   column_offsets    columns used for seq scan
   * @param   predicate         Predicate used in the seq scan
   * @return  true if successfully executes
   */
  bool UpdateWithIndexScan(std::vector<oid_t> update_columns,
                           std::vector<type::Value> update_values,
                           std::vector<type::Value> scan_values,
                           oid_t index_offset,
                           concurrency::TransactionContext *txn);

  /*@brief   Update specific columns using index scan
   * @param   update_columns    Columns to be updated
   * @param   update_values     Values to be updated
   * @param   scan_values       Value to be scaned (used in index scan)
   * @param   index_offset      Offset of index for scan
   * @return  true if successfully executes
   */
  bool  UpdateWithCompiledSeqScan( std::vector<oid_t> update_columns,
                                   std::vector<type::Value> update_values,
                                   std::vector<oid_t> column_offsets,
                                   expression::AbstractExpression *predicate,
                                   concurrency::TransactionContext *txn);

  /*@brief   Add index on catalog table
   * @param   key_attrs    indexed column offset(position)
   * @param   index_oid    index id(global unique)
   * @param   index_name   index name(global unique)
   * @param   index_constraint     index constraints
   * @return  Unique pointer of vector of logical tiles
   * Note: Use catalog::Catalog::CreateIndex() if you can, only ColumnCatalog and
   * IndexCatalog should need this
   */
  void AddIndex(const std::vector<oid_t> &key_attrs, oid_t index_oid,
                const std::string &index_name,
                IndexConstraintType index_constraint);

  //===--------------------------------------------------------------------===//
  // Members
  //===--------------------------------------------------------------------===//

  // Maximum column name size for catalog schemas
  static const size_t max_name_size = 64;
  // which database catalog table is stored int
  oid_t database_oid;
  // Local oid (without catalog type mask) starts from START_OID + OID_OFFSET
  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID + OID_OFFSET);

  storage::DataTable *catalog_table_;
};

}  // namespace catalog
}  // namespace peloton

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

namespace storage {
class Database;
class DataTable;
class Tuple;
}  // namespace storage

namespace catalog {

class AbstractCatalog {
 public:
  virtual ~AbstractCatalog() {}

 protected:
  /* For pg_database, pg_table, pg_index, pg_column */
  AbstractCatalog(storage::Database *pg_catalog,
                  catalog::Schema *catalog_table_schema,
                  oid_t catalog_table_oid,
                  std::string catalog_table_name);

  /* For other catalogs */
  AbstractCatalog(concurrency::TransactionContext *txn,
                  const std::string &catalog_table_ddl);

  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//
  bool InsertTuple(concurrency::TransactionContext *txn,
                   std::unique_ptr<storage::Tuple> tuple);

  bool DeleteWithIndexScan(concurrency::TransactionContext *txn,
                           oid_t index_offset,
                           std::vector<type::Value> values);

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetResultWithIndexScan(
      concurrency::TransactionContext *txn,
      std::vector<oid_t> column_offsets,
      oid_t index_offset,
      std::vector<type::Value> values) const;

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetResultWithSeqScan(
      concurrency::TransactionContext *txn,
      expression::AbstractExpression *predicate,
      std::vector<oid_t> column_offsets);

  bool UpdateWithIndexScan(concurrency::TransactionContext *txn,
                           oid_t index_offset,
                           std::vector<type::Value> scan_values,
                           std::vector<oid_t> update_columns,
                           std::vector<type::Value> update_values);

  void AddIndex(const std::string &index_name,
                oid_t index_oid,
                const std::vector<oid_t> &key_attrs,
                IndexConstraintType index_constraint);

  //===--------------------------------------------------------------------===//
  // Members
  //===--------------------------------------------------------------------===//

  // Maximum column name size for catalog schemas
  static const size_t max_name_size_ = 64;
  // which database catalog table is stored int
  oid_t database_oid_;
  // Local oid (without catalog type mask) starts from START_OID + OID_OFFSET
  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID + OID_OFFSET);

  storage::DataTable *catalog_table_;
};

}  // namespace catalog
}  // namespace peloton

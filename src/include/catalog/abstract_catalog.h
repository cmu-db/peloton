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
}

namespace catalog {

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
  bool InsertTuple(std::unique_ptr<storage::Tuple> tuple,
                   concurrency::TransactionContext *txn);

  bool DeleteWithIndexScan(oid_t index_offset, std::vector<type::Value> values,
                           concurrency::TransactionContext *txn);

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetResultWithIndexScan(std::vector<oid_t> column_offsets, oid_t index_offset,
                         std::vector<type::Value> values,
                         concurrency::TransactionContext *txn) const;

  std::unique_ptr<std::vector<std::unique_ptr<executor::LogicalTile>>>
  GetResultWithSeqScan(std::vector<oid_t> column_offsets,
                       expression::AbstractExpression *predicate,
                       concurrency::TransactionContext *txn);

  void AddIndex(const std::vector<oid_t> &key_attrs, oid_t index_oid,
                const std::string &index_name,
                IndexConstraintType index_constraint);

  //===--------------------------------------------------------------------===//
  // Members
  //===--------------------------------------------------------------------===//

  // Maximum column name size for catalog schemas
  static const size_t max_name_size = 32;

  // Local oid (without catalog type mask) starts from START_OID + OID_OFFSET
  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID + OID_OFFSET);

  storage::DataTable *catalog_table_;
};

}  // namespace catalog
}  // namespace peloton

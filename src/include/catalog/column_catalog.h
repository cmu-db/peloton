//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.h
//
// Identification: src/include/catalog/column_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_attribute
//
// Schema: (column: column_name)
// 0: table_oid (pkey), 1: column_name (pkey), 2: column_offset, 3: column_type,
// 4: is_inlined, 5: is_primary, 6: is_not_null
//
// Indexes: (index offset: indexed columns)
// 0: table_oid & column_name (unique)
// 1: table_oid & column_offset (unique)
// 2: table_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class ColumnCatalog : public AbstractCatalog {
 public:
  // Global Singleton
  static ColumnCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                    type::AbstractPool *pool = nullptr);

  ~ColumnCatalog();

  inline oid_t GetNextOid() { return oid_++ | COLUMN_OID_MASK; }

  // Write related API
  bool InsertColumn(oid_t table_oid, const std::string &column_name,
                    oid_t column_offset, type::Type::TypeId column_type,
                    bool is_inlined, const std::vector<Constraint> &constraints,
                    type::AbstractPool *pool, concurrency::Transaction *txn);
  bool DeleteColumn(oid_t table_oid, const std::string &column_name,
                    concurrency::Transaction *txn);
  bool DeleteColumns(oid_t table_oid, concurrency::Transaction *txn);

  // Read-only API
  oid_t GetColumnOffset(oid_t table_oid, const std::string &column_name,
                        concurrency::Transaction *txn);
  std::string GetColumnName(oid_t table_oid, oid_t column_offset,
                            concurrency::Transaction *txn);
  type::Type::TypeId GetColumnType(oid_t table_oid, std::string column_name,
                                   concurrency::Transaction *txn);
  type::Type::TypeId GetColumnType(oid_t table_oid, oid_t column_offset,
                                   concurrency::Transaction *txn);

 private:
  ColumnCatalog(storage::Database *pg_catalog, type::AbstractPool *pool);

  void AddIndex(const std::vector<oid_t> &key_attrs, oid_t index_oid,
                const std::string &index_name,
                IndexConstraintType index_constraint);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // End catalog namespace
}  // End peloton namespace

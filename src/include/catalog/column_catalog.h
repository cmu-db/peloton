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
// Schema: (column offset: column_name)
// 0: table_oid (pkey)
// 1: column_name (pkey)
// 2: column_id (logical position, starting from 0, then 1, 2, 3....)
// 3: column_offset (physical offset instead of logical position)
// 4: column_type (type of the data stored in this column)
// 5: is_inlined (false for VARCHAR type, true for every type else)
// 6: is_primary (is this column belongs to primary key)
// 7: is_not_null
//
// Indexes: (index offset: indexed columns)
// 0: table_oid & column_name (unique & primary key)
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
  // Global Singleton, only the first call requires passing parameters.
  static ColumnCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                    type::AbstractPool *pool = nullptr,
                                    concurrency::Transaction *txn = nullptr);

  ~ColumnCatalog();

  // No use
  inline oid_t GetNextOid() { return INVALID_OID; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumn(oid_t table_oid, const std::string &column_name,
                    oid_t column_id, oid_t column_offset,
                    type::TypeId column_type, bool is_inlined,
                    const std::vector<Constraint> &constraints,
                    type::AbstractPool *pool, concurrency::Transaction *txn);
  bool DeleteColumn(oid_t table_oid, const std::string &column_name,
                    concurrency::Transaction *txn);
  bool DeleteColumns(oid_t table_oid, concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  oid_t GetColumnOffset(oid_t table_oid, const std::string &column_name,
                        concurrency::Transaction *txn);
  oid_t GetColumnId(oid_t table_oid, const std::string &column_name,
                    concurrency::Transaction *txn);
  std::string GetColumnName(oid_t table_oid, oid_t column_id,
                            concurrency::Transaction *txn);
  type::TypeId GetColumnType(oid_t table_oid, std::string column_name,
                                   concurrency::Transaction *txn);
  type::TypeId GetColumnType(oid_t table_oid, oid_t column_id,
                                   concurrency::Transaction *txn);

 private:
  ColumnCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // namespace catalog
}  // namespace peloton

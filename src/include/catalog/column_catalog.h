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
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class ColumnCatalogObject {
 public:
  ColumnCatalogObject(oid_t table_oid = INVALID_OID)
      : table_oid(table_oid),
        column_name(),
        column_id(-1),
        column_offset(0),
        column_type(type::TypeId::INVALID),
        is_inlined(false),
        is_primary(false),
        is_not_null(false) {}
  ColumnCatalogObject(executor::LogicalTile *tile, int tupleId = 0)
      : table_oid(tile->GetValue(tupleId, 0).GetAs<oid_t>()),
        column_name(tile->GetValue(tupleId, 1).ToString()),
        column_id(tile->GetValue(tupleId, 2).GetAs<oid_t>()),
        column_offset(tile->GetValue(tupleId, 3).GetAs<oid_t>()),
        column_type(StringToTypeId(tile->GetValue(tupleId, 4).ToString())),
        is_inlined(tile->GetValue(tupleId, 5).GetAs<bool>()),
        is_primary(tile->GetValue(tupleId, 6).GetAs<bool>()),
        is_not_null(tile->GetValue(tupleId, 7).GetAs<bool>()) {}

  oid_t table_oid;
  std::string column_name;
  oid_t column_id;
  oid_t column_offset;
  type::TypeId column_type;
  bool is_inlined;
  bool is_primary;
  bool is_not_null;
};

class ColumnCatalog : public AbstractCatalog {
  friend class TableCatalogObject;

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

 private:
  //===--------------------------------------------------------------------===//
  // Read-only Related API(only called within table catalog object)
  //===--------------------------------------------------------------------===//
  const std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
  GetColumnObjects(oid_t table_oid, concurrency::Transaction *txn);

  ColumnCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();
};

}  // namespace catalog
}  // namespace peloton

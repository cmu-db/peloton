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
// 1: table_oid & column_id (unique)
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
  ColumnCatalogObject(executor::LogicalTile *tile, int tupleId = 0);

  inline oid_t GetTableOid() { return table_oid; }
  inline const std::string &GetColumnName() { return column_name; }
  inline oid_t GetColumnId() { return column_id; }
  inline oid_t GetColumnOffset() { return column_offset; }
  inline type::TypeId GetColumnType() { return column_type; }
  inline bool IsInlined() { return is_inlined; }
  inline bool IsPrimary() { return is_primary; }
  inline bool IsNotNull() { return is_not_null; }

 private:
  // member variables
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
  friend class ColumnCatalogObject;
  friend class TableCatalogObject;
  friend class Catalog;

 public:
  // Global Singleton, only the first call requires passing parameters.
  static ColumnCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                    type::AbstractPool *pool = nullptr,
                                    concurrency::TransactionContext *txn = nullptr);

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
                    type::AbstractPool *pool, concurrency::TransactionContext *txn);
  bool DeleteColumn(oid_t table_oid, const std::string &column_name,
                    concurrency::TransactionContext *txn);
  bool DeleteColumns(oid_t table_oid, concurrency::TransactionContext *txn);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API(only called within table catalog object)
  //===--------------------------------------------------------------------===//
  const std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
  GetColumnObjects(oid_t table_oid, concurrency::TransactionContext *txn);

  ColumnCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                concurrency::TransactionContext *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    TABLE_OID = 0,
    COLUMN_NAME = 1,
    COLUMN_ID = 2,
    COLUMN_OFFSET = 3,
    COLUMN_TYPE = 4,
    IS_INLINED = 5,
    IS_PRIMARY = 6,
    IS_NOT_NULL = 7,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1, 2, 3, 4, 5, 6, 7};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_COLUMN_ID = 1,
    SKEY_TABLE_OID = 2,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton

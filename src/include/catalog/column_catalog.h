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

class ColumnCatalogEntry {
 public:
  ColumnCatalogEntry(executor::LogicalTile *tile, int tupleId = 0);

  inline oid_t GetTableOid() { return table_oid_; }
  inline const std::string &GetColumnName() { return column_name_; }
  inline oid_t GetColumnId() { return column_id_; }
  inline oid_t GetColumnOffset() { return column_offset_; }
  inline type::TypeId GetColumnType() { return column_type_; }
  inline size_t GetColumnLength() { return column_length_; }
  inline bool IsInlined() { return is_inlined_; }
  inline bool IsPrimary() { return is_primary_; }
  inline bool IsNotNull() { return is_not_null_; }

 private:
  // member variables
  oid_t table_oid_;
  std::string column_name_;
  oid_t column_id_;
  oid_t column_offset_;
  type::TypeId column_type_;
  size_t column_length_;
  bool is_inlined_;
  bool is_primary_;
  bool is_not_null_;
};

class ColumnCatalog : public AbstractCatalog {
  friend class ColumnCatalogEntry;
  friend class TableCatalogEntry;
  friend class Catalog;

 public:
  ColumnCatalog(concurrency::TransactionContext *txn,
                storage::Database *pg_catalog,
                type::AbstractPool *pool);

  ~ColumnCatalog();

  // No use
  inline oid_t GetNextOid() { return INVALID_OID; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumn(concurrency::TransactionContext *txn,
                    oid_t table_oid,
                    oid_t column_id,
                    const std::string &column_name,
                    oid_t column_offset,
                    type::TypeId column_type,
                    size_t column_length,
                    const std::vector<Constraint> &constraints,
                    bool is_inlined,
                    type::AbstractPool *pool);

  bool DeleteColumn(concurrency::TransactionContext *txn,
                    oid_t table_oid,
                    const std::string &column_name);

  bool DeleteColumns(concurrency::TransactionContext *txn, oid_t table_oid);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API(only called within table catalog object)
  //===--------------------------------------------------------------------===//
  const std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogEntry>>
    GetColumnCatalogEntries(concurrency::TransactionContext *txn,
                            oid_t table_oid);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    TABLE_OID = 0,
    COLUMN_NAME = 1,
    COLUMN_ID = 2,
    COLUMN_OFFSET = 3,
    COLUMN_TYPE = 4,
    COLUMN_LENGTH = 5,
    IS_INLINED = 6,
    IS_PRIMARY = 7,
    IS_NOT_NULL = 8,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3, 4, 5, 6, 7, 8};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_COLUMN_ID = 1,
    SKEY_TABLE_OID = 2,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton

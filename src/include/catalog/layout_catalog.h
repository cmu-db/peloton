//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_catalog.h
//
// Identification: src/include/catalog/layout_catalog.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_layout
//
// Schema: (column offset: column_name)
// 0: table_oid (pkey)
// 1: layout_oid (pkey)
// 2: num_columns (number of columns in the layout)
// 3: column_map (map column_oid to <tile_offset, tile_column_oid>)
//
// Indexes: (index offset: indexed columns)
// 0: table_oid & layout_oid (unique & primary key)
// 1: table_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {

namespace storage {
class Layout;
} // namespace storage

namespace catalog {

class LayoutCatalog : public AbstractCatalog {
  friend class TableCatalogObject;
  friend class Catalog;

public:
  // Global Singleton, only the first call requires passing parameters.
  static LayoutCatalog *GetInstance(storage::Database *pg_catalog = nullptr,
                                    type::AbstractPool *pool = nullptr,
                                    concurrency::TransactionContext *txn = nullptr);

  ~LayoutCatalog();

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertLayout(oid_t table_oid,
                    std::shared_ptr<const storage::Layout> layout,
                    type::AbstractPool *pool,
                    concurrency::TransactionContext *txn);

  bool DeleteLayout(oid_t table_oid, oid_t layout_id,
                    concurrency::TransactionContext *txn);
  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  const std::unordered_map<oid_t, std::shared_ptr<const storage::Layout>>
  GetLayouts(oid_t table_oid, oid_t layout_id,
                   concurrency::TransactionContext *txn);

private:

  LayoutCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                concurrency::TransactionContext *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    TABLE_OID = 0,
    LAYOUT_OID = 1,
    NUM_COLUMNS = 2,
    COLUMN_MAP = 3,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1, 2, 3};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_TABLE_OID = 1,
    // Add new indexes here in creation order
  };
};


}  // namespace catalog
}  // namespace peloton
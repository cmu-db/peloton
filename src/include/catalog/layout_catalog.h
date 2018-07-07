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

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {

namespace storage {
class Layout;
}  // namespace storage

namespace catalog {

class LayoutCatalog : public AbstractCatalog {
 public:

  LayoutCatalog(concurrency::TransactionContext *txn,
                storage::Database *pg_catalog,
                type::AbstractPool *pool);

  ~LayoutCatalog();

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertLayout(concurrency::TransactionContext *txn,
                    oid_t table_oid,
                    std::shared_ptr<const storage::Layout> layout,
                    type::AbstractPool *pool);

  bool DeleteLayout(concurrency::TransactionContext *txn,
                    oid_t table_oid,
                    oid_t layout_oid);

  bool DeleteLayouts(concurrency::TransactionContext *txn, oid_t table_oid);

  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  const std::unordered_map<oid_t, std::shared_ptr<const storage::Layout>>
  GetLayouts(concurrency::TransactionContext *txn, oid_t table_oid);

  std::shared_ptr<const storage::Layout> GetLayoutWithOid(concurrency::TransactionContext *txn,
                                                          oid_t table_oid,
                                                          oid_t layout_oid);

 private:
  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    TABLE_OID = 0,
    LAYOUT_OID = 1,
    NUM_COLUMNS = 2,
    COLUMN_MAP = 3,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_TABLE_OID = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
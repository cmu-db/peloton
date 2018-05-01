//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema_catalog.h
//
// Identification: src/include/catalog/schema_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_namespace
//
// Schema: (column offset: column_name)
// 0: schema_oid (pkey)
// 1: schema_name
//
// Indexes: (index offset: indexed columns)
// 0: schema_oid (unique & primary key)
// 1: schema_name (unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class SchemaCatalogObject {
  friend class DatabaseCatalogObject;

 public:
  SchemaCatalogObject(executor::LogicalTile *tile,
                      concurrency::TransactionContext *txn);

  inline oid_t GetSchemaOid() { return schema_oid; }
  inline const std::string &GetSchemaName() { return schema_name; }

 private:
  // member variables
  oid_t schema_oid;
  std::string schema_name;
  // Pointer to its corresponding transaction
  // This object is only visible during this transaction
  concurrency::TransactionContext *txn;
};

class SchemaCatalog : public AbstractCatalog {
  friend class SchemaCatalogObject;
  friend class Catalog;

 public:
  SchemaCatalog(storage::Database *peloton, type::AbstractPool *pool,
                concurrency::TransactionContext *txn);

  ~SchemaCatalog();

  inline oid_t GetNextOid() { return oid_++ | SCHEMA_OID_MASK; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertSchema(oid_t schema_oid, const std::string &schema_name,
                    type::AbstractPool *pool,
                    concurrency::TransactionContext *txn);
  bool DeleteSchema(const std::string &schema_name,
                    concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<SchemaCatalogObject> GetSchemaObject(
      const std::string &schema_name, concurrency::TransactionContext *txn);

 private:
  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    SCHEMA_OID = 0,
    SCHEMA_NAME = 1,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_SCHEMA_NAME = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_storage_manager.h
//
// Identification: src/include/catalog/catalog_storage_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_defaults.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/schema.h"
#include "catalog/table_catalog.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "type/abstract_pool.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"

namespace peloton {


namespace storage {
class DataTable;
class Database;
}

namespace catalog {


class CatalogStorageManager {
 public:
  // Global Singleton
  static CatalogStorageManager *GetInstance(void);

  // Deconstruct the catalog database when destroying the catalog.
  ~CatalogStorageManager();

  //===--------------------------------------------------------------------===//
  // GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
  //===--------------------------------------------------------------------===//

  /* Find a database using its oid from storage layer,
   * throw exception if not exists
   * */
  storage::Database *GetDatabaseWithOid(oid_t db_oid) const;

  /* Find a table using its oid from storage layer,
   * throw exception if not exists
   * */
  storage::DataTable *GetTableWithOid(oid_t database_oid,
                                      oid_t table_oid) const;

  /* Find a index using its oid from storage layer,
   * throw exception if not exists
   * */
  index::Index *GetIndexWithOid(oid_t database_oid, oid_t table_oid,
                                oid_t index_oid) const;

  //===--------------------------------------------------------------------===//
  // HELPERS
  //===--------------------------------------------------------------------===//
  // Returns true if the catalog contains the given database with the id
  bool HasDatabase(oid_t db_oid) const;
  oid_t GetDatabaseCount() { return databases_.size(); }

 private:
  CatalogStorageManager();

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;

};
}
}

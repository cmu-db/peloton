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

#include "catalog/database_catalog.h"
#include "catalog/schema.h"
#include "storage/data_table.h"
#include "storage/database.h"

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
  // DEPRECATED FUNCTIONs
  //===--------------------------------------------------------------------===//
  /*
  * We're working right now to remove metadata from storage level and eliminate
  * multiple copies, so those functions below will be DEPRECATED soon.
  */

  // Find a database using vector offset
  storage::Database *GetDatabaseWithOffset(oid_t database_offset) const;

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

  //===--------------------------------------------------------------------===//
  // FUNCTIONS USED BY CATALOG
  //===--------------------------------------------------------------------===//

  void AddDatabaseToStorageManager(storage::Database *db) {
    databases_.push_back(db);
  }
  bool RemoveDatabaseFromStorageManager(oid_t database_oid) {
    for (auto it = databases_.begin(); it != databases_.end(); ++it) {
      if ((*it)->GetOid() == database_oid) {
        delete (*it);
        databases_.erase(it);
        return true;
      }
    }
    return false;
  }

  void DestroyDatabases();

 private:
  CatalogStorageManager();

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;
};
}
}

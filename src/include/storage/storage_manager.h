//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager.h
//
// Identification: src/include/storage/storage_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {

class Database;
class DataTable;

class StorageManager {
 public:
  // Global Singleton
  static StorageManager *GetInstance(void);

  // Deconstruct the catalog database when destroying the catalog.
  ~StorageManager();

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

  bool RemoveDatabaseFromStorageManager(oid_t database_oid);

  void DestroyDatabases();

 private:
  StorageManager();

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;
};

}  // namespace
}

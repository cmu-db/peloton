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
#include <atomic>
#include "common/container/cuckoo_map.h"
#include "common/internal_types.h"
#include "storage/tile_group.h"

namespace peloton {

namespace storage {
  class TileGroup;
  class IndirectionArray;
}

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


  //===--------------------------------------------------------------------===//
  // TILE GROUP ALLOCATION
  //===--------------------------------------------------------------------===//

  oid_t GetNextTileId() { return ++tile_oid_; }

  oid_t GetNextTileGroupId() { return ++tile_group_oid_; }

  oid_t GetCurrentTileGroupId() { return tile_group_oid_; }

  void SetNextTileGroupId(oid_t next_oid) { tile_group_oid_ = next_oid; }

  void AddTileGroup(const oid_t oid,
                    std::shared_ptr<storage::TileGroup> location);

  void DropTileGroup(const oid_t oid);

  std::shared_ptr<storage::TileGroup> GetTileGroup(const oid_t oid);

  void ClearTileGroup(void);

 private:
  StorageManager();

  // A vector of the database pointers in the catalog
  std::vector<storage::Database *> databases_;

  //===--------------------------------------------------------------------===//
  // Data member for tile allocation
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> tile_oid_ = ATOMIC_VAR_INIT(START_OID);

  //===--------------------------------------------------------------------===//
  // Data members for tile group allocation
  //===--------------------------------------------------------------------===//
  std::atomic<oid_t> tile_group_oid_ = ATOMIC_VAR_INIT(START_OID);

  CuckooMap<oid_t, std::shared_ptr<storage::TileGroup>> tile_group_locator_;
  static std::shared_ptr<storage::TileGroup> empty_tile_group_;
};

}  // namespace
}

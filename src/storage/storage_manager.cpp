//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager.cpp
//
// Identification: src/storage/storage_manager.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/storage_manager.h"

#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"

namespace peloton {
namespace storage {

std::shared_ptr<storage::TileGroup> StorageManager::empty_tile_group_;

StorageManager::StorageManager() = default;

StorageManager::~StorageManager() = default;

// Get instance of the global catalog storage manager
StorageManager *StorageManager::GetInstance() {
  static StorageManager global_catalog_storage_manager;
  return &global_catalog_storage_manager;
}

//===--------------------------------------------------------------------===//
// GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
//===--------------------------------------------------------------------===//

/* Find a database using its oid from storage layer,
 * throw exception if not exists
 * */
Database *StorageManager::GetDatabaseWithOid(
    oid_t database_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == database_oid) return database;
  throw CatalogException("Database with oid = " + std::to_string(database_oid) +
                         " is not found");
  return nullptr;
}

/* Find a table using its oid from storage layer,
 * throw exception if not exists
 * */
DataTable *StorageManager::GetTableWithOid(
    oid_t database_oid, oid_t table_oid) const {
  LOG_TRACE("Getting table with oid %d from database with oid %d", database_oid,
            table_oid);
  // Lookup DB from storage layer
  auto database =
      GetDatabaseWithOid(database_oid);  // Throw exception if not exists
  // Lookup table from storage layer
  return database->GetTableWithOid(table_oid);  // Throw exception if not exists
}

/* Find a index using its oid from storage layer,
 * throw exception if not exists
 * */
index::Index *StorageManager::GetIndexWithOid(oid_t database_oid,
                                              oid_t table_oid,
                                              oid_t index_oid) const {
  // Lookup table from storage layer
  auto table = GetTableWithOid(database_oid,
                               table_oid);  // Throw exception if not exists
  // Lookup index from storage layer
  return table->GetIndexWithOid(index_oid)
      .get();  // Throw exception if not exists
}

//===--------------------------------------------------------------------===//
// DEPRECATED
//===--------------------------------------------------------------------===//

// This is used as an iterator
Database *StorageManager::GetDatabaseWithOffset(
    oid_t database_offset) const {
  PELOTON_ASSERT(database_offset < databases_.size());
  auto database = databases_.at(database_offset);
  return database;
}

//===--------------------------------------------------------------------===//
// HELPERS
//===--------------------------------------------------------------------===//

// Only used for testing
bool StorageManager::HasDatabase(oid_t db_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == db_oid) return (true);
  return (false);
}

//Invoked when catalog is destroyed
void StorageManager::DestroyDatabases() {
  LOG_TRACE("Deleting databases");
  for (auto database : databases_) delete database;
  LOG_TRACE("Finish deleting database");
}

bool StorageManager::RemoveDatabaseFromStorageManager(oid_t database_oid) {
  for (auto it = databases_.begin(); it != databases_.end(); ++it) {
    if ((*it)->GetOid() == database_oid) {
      delete (*it);
      databases_.erase(it);
      return true;
    }
  }
  return false;
}


//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void StorageManager::AddTileGroup(const oid_t &oid,
                           std::shared_ptr<storage::TileGroup> location) {
  // do this check first, so that count is not updated yet
  if (!tile_group_locator_.Contains(oid)) {
    // only increment if new tile group
    num_live_tile_groups_.fetch_add(1);
  }
  tile_group_locator_.Upsert(oid, location);
}

void StorageManager::DropTileGroup(const oid_t &oid) {
  // drop the catalog reference to the tile group
  if (tile_group_locator_.Erase(oid)) {
    num_live_tile_groups_.fetch_sub(1);
  }
}

std::shared_ptr<storage::TileGroup> StorageManager::GetTileGroup(const oid_t &oid) {
  std::shared_ptr<storage::TileGroup> location;
  if (tile_group_locator_.Find(oid, location)) {
    return location;
  }
  return empty_tile_group_;
}

// used for logging test
void StorageManager::ClearTileGroup() {
  num_live_tile_groups_.store(0);
  tile_group_locator_.Clear();
}

}  // namespace storage
}  // namespace peloton

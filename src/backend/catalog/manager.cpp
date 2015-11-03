//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// manager.cpp
//
// Identification: src/backend/catalog/manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "backend/catalog/manager.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace catalog {

Manager &Manager::GetInstance() {
  static Manager manager;
  return manager;
}

//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void Manager::SetTileGroup(const oid_t oid, storage::TileGroup *location) {
  {
    std::lock_guard<std::mutex> lock(locator_mutex);
    locator.insert(std::pair<oid_t, storage::TileGroup *>(oid, location));
  }
}

storage::TileGroup *Manager::GetTileGroup(const oid_t oid) {
  storage::TileGroup *location = nullptr;

  {
    std::lock_guard<std::mutex> lock(locator_mutex);
    if (locator.find(oid) != locator.end())
      location = locator.at(oid);
  }

  return location;
}

//used for logging test
void Manager::ClearTileGroup(){
  {
    std::lock_guard<std::mutex> lock(locator_mutex);
    locator.clear();
  }
}

//===--------------------------------------------------------------------===//
// DATABASE
//===--------------------------------------------------------------------===//

void Manager::AddDatabase(storage::Database *database) {
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);
    databases.push_back(database);
  }
}

storage::Database *Manager::GetDatabaseWithOid(const oid_t database_oid) const {
  for (auto database : databases)
    if (database->GetOid() == database_oid) return database;

  return nullptr;
}

void Manager::DropDatabaseWithOid(const oid_t database_oid) {
  {
    std::lock_guard<std::mutex> lock(catalog_mutex);

    oid_t database_offset = 0;
    for (auto database : databases) {
      if (database->GetOid() == database_oid) {
        delete database;
        break;
      }
      database_offset++;
    }
    assert(database_offset < databases.size());

    // Drop the database
    databases.erase(databases.begin() + database_offset);
  }
}

storage::Database *Manager::GetDatabase(const oid_t database_offset) const {
  assert(database_offset < databases.size());
  auto database = databases.at(database_offset);
  return database;
}

oid_t Manager::GetDatabaseCount() const { return databases.size(); }

//===--------------------------------------------------------------------===//
// CONVENIENCE WRAPPERS
//===--------------------------------------------------------------------===//

storage::DataTable *Manager::GetTableWithOid(const oid_t database_oid,
                                             const oid_t table_oid) const {
  // Lookup DB
  auto database = GetDatabaseWithOid(database_oid);

  // Lookup table
  if (database != nullptr) {
    auto table = database->GetTableWithOid(table_oid);
    return table;
  }

  return nullptr;
}

storage::DataTable *Manager::GetTableWithName(
    const oid_t database_oid, const std::string table_name) const {
  // Lookup DB
  auto database = GetDatabaseWithOid(database_oid);

  // Lookup table
  if (database != nullptr) {
    auto table = database->GetTableWithName(table_name);
    return table;
  }

  return nullptr;
}

index::Index *Manager::GetIndexWithOid(const oid_t database_oid,
                                       const oid_t table_oid,
                                       const oid_t index_oid) const {
  // Lookup table
  auto table = GetTableWithOid(database_oid, table_oid);

  // Lookup index
  if (table != nullptr) {
    auto index = table->GetIndexWithOid(index_oid);
    return index;
  }

  return nullptr;
}

}  // End catalog namespace
}  // End peloton namespace

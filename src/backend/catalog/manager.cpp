//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager.cpp
//
// Identification: src/backend/catalog/manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <string>

#include "backend/common/exception.h"
#include "backend/catalog/manager.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/concurrency/transaction_manager_factory.h"

namespace peloton {

namespace concurrency {
  class TransactionManagerFactory;
}

namespace catalog {

Manager &Manager::GetInstance() {
  static Manager manager;
  return manager;
}

//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void Manager::AddTileGroup(
    const oid_t oid, const std::shared_ptr<storage::TileGroup> &location) {

  // drop the catalog reference to the old tile group
  locator.erase(oid);

  // add a catalog reference to the tile group
  locator[oid] = location;
}

void Manager::DropTileGroup(const oid_t oid) {
  concurrency::TransactionManagerFactory::GetInstance().DroppingTileGroup(oid);
  {
    LOG_TRACE("Dropping tile group %u", oid);
    // std::lock_guard<std::mutex> lock(locator_mutex);
    // drop the catalog reference to the tile group
    locator.erase(oid);
  }
}

std::shared_ptr<storage::TileGroup> Manager::GetTileGroup(const oid_t oid) {
  std::shared_ptr<storage::TileGroup> location;

  locator.find(oid, location);

  return location;
}

// used for logging test
void Manager::ClearTileGroup() {
  locator.clear();
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
  for (auto database : databases) {
    if (database->GetOid() == database_oid) return database;
  }

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

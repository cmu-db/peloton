//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager.cpp
//
// Identification: src/catalog/manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/exception.h"
#include "common/logger.h"
#include "catalog/manager.h"
#include "catalog/foreign_key.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace catalog {

std::shared_ptr<storage::TileGroup> Manager::empty_location;

Manager &Manager::GetInstance() {
  static Manager manager;
  return manager;
}

//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void Manager::AddTileGroup(const oid_t oid,
                           std::shared_ptr<storage::TileGroup> location) {

  {
    // add/update the catalog reference to the tile group
    locator.Update(oid, location);
  }

}

void Manager::DropTileGroup(const oid_t oid) {
  concurrency::TransactionManagerFactory::GetInstance().DroppingTileGroup(oid);
  LOG_INFO("Dropping tile group %u", oid);

  {
    // drop the catalog reference to the tile group
    locator.Erase(oid, empty_location);
  }

}

std::shared_ptr<storage::TileGroup> Manager::GetTileGroup(const oid_t oid) {
  std::shared_ptr<storage::TileGroup> location;

  {
    location = locator.Find(oid);
  }

  return location;
}

// used for logging test
void Manager::ClearTileGroup() {

  {
    locator.Clear(empty_location);
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
    PL_ASSERT(database_offset < databases.size());

    // Drop the database
    databases.erase(databases.begin() + database_offset);
  }
}

storage::Database *Manager::GetDatabase(const oid_t database_offset) const {
  PL_ASSERT(database_offset < databases.size());
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

}  // End catalog namespace
}  // End peloton namespace

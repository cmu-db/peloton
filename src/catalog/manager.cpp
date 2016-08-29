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

  {
    // drop the catalog reference to the tile group
    locator.Erase(oid, empty_location);
  }
}

std::shared_ptr<storage::TileGroup> Manager::GetTileGroup(const oid_t oid) {
  std::shared_ptr<storage::TileGroup> location;

  { location = locator.Find(oid); }

  return location;
}

// used for logging test
void Manager::ClearTileGroup() {

  { locator.Clear(empty_location); }
}

}  // End catalog namespace
}  // End peloton namespace

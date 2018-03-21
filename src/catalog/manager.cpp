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
#include "common/container/cuckoo_map.h"

namespace peloton {
namespace catalog {

Manager &Manager::GetInstance() {
  static Manager manager;
  return manager;
}

//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void Manager::AddTileGroup(const oid_t oid,
                           std::shared_ptr<storage::TileGroup> location) {
  // add/update the catalog reference to the tile group
  bool status = tile_group_locator_.Insert(oid, location);
  while (!status) {
    tile_group_locator_.Erase(oid);
    status = tile_group_locator_.Insert(oid, location);
  }
}

void Manager::DropTileGroup(const oid_t oid) {
  // drop the catalog reference to the tile group
  tile_group_locator_.Erase(oid);
}

std::shared_ptr<storage::TileGroup> Manager::GetTileGroup(const oid_t oid) {
  std::shared_ptr<storage::TileGroup> location;
  tile_group_locator_.Find(oid, location);
  return location;
}

// used for logging test
void Manager::ClearTileGroup() { tile_group_locator_.Clear(); }

void Manager::AddIndirectionArray(
    const oid_t oid, std::shared_ptr<storage::IndirectionArray> location) {
  // add/update the catalog reference to the indirection array
  indirection_array_locator_.Update(oid, location);
}

void Manager::DropIndirectionArray(const oid_t oid) {
  // drop the catalog reference to the tile group
  indirection_array_locator_.Erase(oid);
}

// used for logging test
void Manager::ClearIndirectionArray() { indirection_array_locator_.Clear(); }

}  // namespace catalog
}  // namespace peloton

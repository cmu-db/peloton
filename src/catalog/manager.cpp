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
#include "storage/database.h"
#include "storage/data_table.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace catalog {

std::shared_ptr<storage::TileGroup> Manager::empty_tile_group_;

std::shared_ptr<storage::IndirectionArray> Manager::empty_indirection_array_;

Manager::Manager() : tile_group_locator_(DEFAULT_LOCATOR_SIZE) {}

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
  tile_group_locator_.Upsert(oid, location);
}

void Manager::DropTileGroup(const oid_t oid) {
  // drop the catalog reference to the tile group
  tile_group_locator_.Erase(oid);
}

std::shared_ptr<storage::TileGroup> Manager::GetTileGroup(const oid_t oid) {
  std::shared_ptr<storage::TileGroup> location;
  if (tile_group_locator_.Find(oid, location)) {
    return location;
  }
  return empty_tile_group_;
}

// used for logging test
void Manager::ClearTileGroup() { tile_group_locator_.Clear(); }

void Manager::AddIndirectionArray(
    const oid_t oid, std::shared_ptr<storage::IndirectionArray> location) {
  // add/update the catalog reference to the indirection array
  auto ret = indirection_array_locator_[oid] = location;
}

void Manager::DropIndirectionArray(const oid_t oid) {
  // drop the catalog reference to the tile group
  indirection_array_locator_[oid] = empty_indirection_array_;
}

// used for logging test
void Manager::ClearIndirectionArray() { indirection_array_locator_.clear(); }

}  // namespace catalog
}  // namespace peloton

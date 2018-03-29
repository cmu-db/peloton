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
  auto value = tbb::concurrent_unordered_map<
      oid_t, std::shared_ptr<storage::TileGroup>>::value_type(oid, location);
  auto ret = tile_group_locator_.insert(value);
  if (!ret.second) {
    tile_group_locator_.find(oid)->second = location;
  }
}

void Manager::DropTileGroup(const oid_t oid) {
  // drop the catalog reference to the tile group
  tile_group_locator_.unsafe_erase(oid);
}

std::shared_ptr<storage::TileGroup> Manager::GetTileGroup(const oid_t oid) {
  auto location = tile_group_locator_.find(oid)->second;
  return location;
}

// used for logging test
void Manager::ClearTileGroup() { tile_group_locator_.clear(); }

void Manager::AddIndirectionArray(
    const oid_t oid, std::shared_ptr<storage::IndirectionArray> location) {
  // add/update the catalog reference to the indirection array
  auto value = tbb::concurrent_unordered_map<
      oid_t, std::shared_ptr<storage::IndirectionArray>>::value_type(oid,
                                                                     location);
  auto ret = indirection_array_locator_.insert(value);
  if (!ret.second) {
    indirection_array_locator_.find(oid)->second = location;
  }
}

void Manager::DropIndirectionArray(const oid_t oid) {
  // drop the catalog reference to the tile group
  indirection_array_locator_.unsafe_erase(oid);
}

// used for logging test
void Manager::ClearIndirectionArray() { indirection_array_locator_.clear(); }

}  // namespace catalog
}  // namespace peloton

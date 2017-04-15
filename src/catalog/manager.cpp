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

std::shared_ptr<storage::TileGroup> Manager::empty_tile_group_;

std::shared_ptr<storage::IndirectionArray> Manager::empty_indirection_array_;

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
  tile_group_locator_.Update(oid, location);
}

void Manager::DropTileGroup(const oid_t oid) {
  // drop the catalog reference to the tile group
  tile_group_locator_.Erase(oid, empty_tile_group_);
}

std::shared_ptr<storage::TileGroup> Manager::GetTileGroup(const oid_t oid) {
  std::shared_ptr<storage::TileGroup> location;

  location = tile_group_locator_.Find(oid);

  return location;
}

// used for logging test
void Manager::ClearTileGroup() { tile_group_locator_.Clear(empty_tile_group_); }

void Manager::AddIndirectionArray(
    const oid_t oid, std::shared_ptr<storage::IndirectionArray> location) {
  // add/update the catalog reference to the indirection array
  indirection_array_locator_.Update(oid, location);
}

void Manager::DropIndirectionArray(const oid_t oid) {
  // drop the catalog reference to the tile group
  indirection_array_locator_.Erase(oid, empty_indirection_array_);
}

// used for logging test
void Manager::ClearIndirectionArray() {
  indirection_array_locator_.Clear(empty_indirection_array_);
}

}  // End catalog namespace
}  // End peloton namespace

//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logical_tile.cpp
//
// Identification: src/backend/executor/logical_tile.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/logical_tile.h"

#include <cassert>
#include <iostream>

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace executor {

LogicalTile::~LogicalTile() {
  // Drop reference on base tiles.
  for (storage::Tile *base_tile : base_tiles_) {
    base_tile->DecrementRefCount();
  }
}

/**
 * @brief Adds column metadata to the logical tile.
 * @param base_tile Base tile that this column is from.
 * @param own_base_tile True if the logical tile should assume ownership of
 *                      the base tile passed in.
 * @param origin_column_id Original column id of this column in its base tile.
 * @param position_list_idx Index of the position list corresponding to this
 *        column.
 *
 * The position list corresponding to this column should be added
 * before the metadata.
 */
void LogicalTile::AddColumn(storage::Tile *base_tile, bool own_base_tile,
                            oid_t origin_column_id, oid_t position_list_idx) {
  assert(position_list_idx < position_lists_.size());

  ColumnInfo cp;
  cp.base_tile = base_tile;
  cp.origin_column_id = origin_column_id;
  cp.position_list_idx = position_list_idx;
  schema_.push_back(cp);

  // Add a reference to the base tile
  auto ret = base_tiles_.insert(base_tile);
  if (ret.second == true) {
    std::cout << "Inc :: " << base_tile << " [ " << base_tile->GetRefCount() << " ] \n";
    base_tile->IncrementRefCount();

    // Drop a reference if you own the base tile
    if(own_base_tile == true) {
      base_tile->DecrementRefCount();
    }
  }

}

/**
 * @brief Add the column specified in column_ids to this logical tile.
 */
void LogicalTile::AddColumns(storage::TileGroup *tile_group, const std::vector<oid_t> &column_ids) {
  const int position_list_idx = 0;
  const bool own_base_tile = false;
  for (oid_t origin_column_id : column_ids) {
    oid_t base_tile_offset, tile_column_id;

    tile_group->LocateTileAndColumn(origin_column_id, base_tile_offset,
                                    tile_column_id);

    AddColumn(tile_group->GetTile(base_tile_offset),
                            own_base_tile, tile_column_id, position_list_idx);
  }
}

/**
 * @brief Transfer all owned basetiles to another logical tile
 *        and give up all the base tiles
 *
 */
void LogicalTile::TransferOwnershipTo(LogicalTile *other) {
  auto other_ownership_set = other->GetOwnedBaseTiles();
  other_ownership_set.insert(base_tiles_.begin(),
                             base_tiles_.end());
  base_tiles_.clear();
}

/**
 * @brief Get the underlying owned base tile set
 * @return Owned base tile set of the tile
 */
std::unordered_set<storage::Tile *> &LogicalTile::GetOwnedBaseTiles() {
  return base_tiles_;
}

/**
 * @brief Given the original column ids, reorganize the schema to conform the new column_ids
 * column_ids is a vector of oid_t. Each column_id is the index into the original table schema
 * schema_ is a vector of ColumnInfos. Each ColumnInfo represents a column in the corresponding place in colum_ids.
 */
void LogicalTile::ProjectColumns(const std::vector<oid_t> &original_column_ids, const std::vector<oid_t> &column_ids) {
  std::vector<ColumnInfo> new_schema;
  for (auto id : column_ids) {
    auto ret = std::find(original_column_ids.begin(), original_column_ids.end(), id);
    assert(ret != original_column_ids.end());
    new_schema.push_back(schema_[*ret]);
  }

  // remove ownership if needed
  for (auto it = base_tiles_.begin(); it != base_tiles_.end();) {
    for (auto cp : new_schema) {
      if (cp.base_tile == *it) {
        it++;
        continue;
      }
    }
    // Drop reference
    (*it)->DecrementRefCount();
    it = base_tiles_.erase(it);
  }

  schema_ = std::move(new_schema);
}

/** @brief Returns a string representation of this tile. */
std::ostream &operator<<(std::ostream &os, const LogicalTile &lt) {
  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  os << "\t-----------------------------------------------------------\n";
  os << "\t SCHEMA : \n";
  for (unsigned int i = 0; i < lt.schema_.size(); i++) {
    const LogicalTile::ColumnInfo &cp = lt.schema_[i];
    os << "\t Position list idx: " << cp.position_list_idx << ", "
       << "base tile: " << cp.base_tile << ", " << "origin column id: "
       << cp.origin_column_id << std::endl;
  }

  os << "\t-----------------------------------------------------------\n";
  os << "\t VISIBLE ROWS : ";

  for (unsigned int i = 0; i < lt.total_tuples_; i++) {
    os << lt.visible_rows_[i] << " ";
  }

  os << std::endl;

  os << "\t-----------------------------------------------------------\n";
  os << "\t POSITION LISTS : \n";

  int pos_list_id = 0;
  for (auto position_list : lt.position_lists_) {
    os << "\t " << pos_list_id++ << " : ";
    for (auto pos : position_list) {
      os << pos << " ";
    }
    os << "\n";
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
}

}  // End executor namespace
}  // End peloton namespace

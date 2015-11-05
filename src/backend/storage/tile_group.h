//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_group.h
//
// Identification: src/backend/storage/tile_group.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/manager.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group_header.h"

#include <cassert>

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group
//===--------------------------------------------------------------------===//

class AbstractTable;
class TileGroupIterator;

typedef std::map<oid_t, std::pair<oid_t, oid_t>> column_map_type;

/**
 * Represents a group of tiles logically horizontally contiguous.
 *
 * < <Tile 1> <Tile 2> .. <Tile n> >
 *
 * Look at TileGroupHeader for MVCC implementation.
 *
 * TileGroups are only instantiated via TileGroupFactory.
 */
class TileGroup {
  friend class Tile;
  friend class TileGroupFactory;

  TileGroup() = delete;
  TileGroup(TileGroup const &) = delete;

 public:
  // Tile group constructor
  TileGroup(TileGroupHeader *tile_group_header, AbstractTable *table,
            const std::vector<catalog::Schema> &schemas,
            const column_map_type &column_map, int tuple_count);

  ~TileGroup();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  // insert tuple at next available slot in tile if a slot exists
  oid_t InsertTuple(txn_id_t transaction_id, const Tuple *tuple);

  // insert tuple at specific tuple slot
  // used by recovery mode
  oid_t InsertTuple(txn_id_t transaction_id, oid_t tuple_slot_id, const Tuple *tuple);


  // returns tuple at given slot in tile if it exists
  Tuple *SelectTuple(oid_t tile_offset, oid_t tuple_slot_id);

  // returns tuple at given slot if it exists
  Tuple *SelectTuple(oid_t tuple_slot_id);

  // delete tuple at given slot if it is not already locked
  bool DeleteTuple(txn_id_t transaction_id, oid_t tuple_slot_id, cid_t last_cid);

  //===--------------------------------------------------------------------===//
  // Transaction Processing
  //===--------------------------------------------------------------------===//

  // commit the inserted tuple
  void CommitInsertedTuple(oid_t tuple_slot_id, cid_t commit_id, txn_id_t transaction_id);

  // commit the deleted tuple
  void CommitDeletedTuple(oid_t tuple_slot_id, txn_id_t transaction_id,
                          cid_t commit_id);

  // abort the inserted tuple
  void AbortInsertedTuple(oid_t tuple_slot_id);

  // abort the deleted tuple
  void AbortDeletedTuple(oid_t tuple_slot_id, txn_id_t transaction_id);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation of this tile group
  friend std::ostream &operator<<(std::ostream &os,
                                  const TileGroup &tile_group);

  oid_t GetNextTupleSlot() const {
    return tile_group_header->GetNextTupleSlot();
  }

  oid_t GetActiveTupleCount() const {
    return tile_group_header->GetActiveTupleCount();
  }

  oid_t GetAllocatedTupleCount() const { return num_tuple_slots; }

  TileGroupHeader *GetHeader() const { return tile_group_header; }

  void SetHeader(TileGroupHeader *header) { tile_group_header = header; }

  unsigned int NumTiles() const { return tiles.size(); }

  // Get the tile at given offset in the tile group
  Tile *GetTile(const oid_t tile_itr) const;

  oid_t GetTileId(const oid_t tile_id) const {
    assert(tiles[tile_id]);
    return tiles[tile_id]->GetTileId();
  }

  peloton::VarlenPool *GetTilePool(const oid_t tile_id) const {
    Tile *tile = GetTile(tile_id);

    if (tile != nullptr) return tile->GetPool();

    return nullptr;
  }

  const std::map<oid_t, std::pair<oid_t, oid_t>> &GetColumnMap() const {
    return column_map;
  }

  oid_t GetTileGroupId() const { return tile_group_id; }

  oid_t GetDatabaseId() const { return database_id; }

  oid_t GetTableId() const { return table_id; }

  AbstractTable *GetAbstractTable() const { return table; }

  void SetTileGroupId(oid_t tile_group_id_) { tile_group_id = tile_group_id_; }

  std::vector<catalog::Schema> &GetTileSchemas() { return tile_schemas; }

  size_t GetTileCount() const { return tile_count; }

  void LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                           oid_t &tile_column_offset);

  oid_t GetTileIdFromColumnId(oid_t column_id);

  oid_t GetTileColumnId(oid_t column_id);

  Value GetValue(oid_t tuple_id, oid_t column_id);

  void IncrementRefCount();

  void DecrementRefCount();

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Catalog information
  oid_t database_id;
  oid_t table_id;
  oid_t tile_group_id;

  // mapping to tile schemas
  std::vector<catalog::Schema> tile_schemas;

  // set of tiles
  std::vector<Tile *> tiles;

  // associated tile group
  TileGroupHeader *tile_group_header;

  // associated table
  AbstractTable *table;  // TODO: Remove this! It is a waste of space!!

  // number of tuple slots allocated
  oid_t num_tuple_slots;

  // number of tiles
  oid_t tile_count;

  std::mutex tile_group_mutex;

  // column to tile mapping :
  // <column offset> to <tile offset, tile column offset>
  column_map_type column_map;

  // references
  std::atomic<size_t> ref_count;

};

}  // End storage namespace
}  // End peloton namespace

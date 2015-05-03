/*-------------------------------------------------------------------------
 *
 * tile_group.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_group.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/manager.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include <cassert>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group
//===--------------------------------------------------------------------===//

class TileGroupIterator;

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
  TileGroup(TileGroup const&) = delete;

 public:

  // Tile group constructor
  TileGroup(TileGroupHeader *tile_group_header,
            Backend *backend,
            const std::vector<catalog::Schema>& schemas,
            int tuple_count);

  ~TileGroup() {
    // clean up tiles
    for(auto tile : tiles)
      delete tile;

    // clean up tile group header
    delete tile_group_header;
  }

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  // insert tuple at next available slot in tile if a slot exists
  id_t InsertTuple(txn_id_t transaction_id, const Tuple *tuple);

  // returns tuple at given slot if it exists and is visible to transaction at this time stamp
  Tuple* SelectTuple(txn_id_t transaction_id, id_t tile_id, id_t tuple_slot_id, cid_t at_cid);

  // returns tuples present in tile and are visible to transaction at this time stamp
  Tile* ScanTuples(txn_id_t transaction_id, id_t tile_id, cid_t at_cid);

  // delete tuple at given slot if it is not already locked
  bool DeleteTuple(txn_id_t transaction_id, id_t tuple_slot_id);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation of this tile group
  friend std::ostream& operator<<(std::ostream& os, const TileGroup& tile_group);

  id_t GetActiveTupleCount() const {
    return tile_group_header->GetActiveTupleCount();
  }

  TileGroupHeader *GetHeader() const{
    return tile_group_header;
  }

  unsigned int NumTiles() const {
    return tiles.size();
  }

  Tile *GetTile(const id_t tile_id) const {
    return tiles[tile_id];
  }

  id_t GetTileId(const id_t tile_id) const {
    return tiles[tile_id]->GetTileId();
  }

  Pool *GetTilePool(const id_t tile_id) const {
    return tiles[tile_id]->GetPool();
  }

  id_t GetTileGroupId() const {
    return tile_group_id;
  }

  std::vector<catalog::Schema> &GetTileSchemas() {
    return tile_schemas;
  }

 protected:

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Catalog information
  id_t database_id;
  id_t table_id;
  id_t tile_group_id;

  // backend
  Backend* backend;

  // mapping to tile schemas
  std::vector<catalog::Schema> tile_schemas;

  // set of tiles
  std::vector<Tile*> tiles;

  TileGroupHeader* tile_group_header;

  // number of tuple slots allocated
  id_t num_tuple_slots;

  // number of tiles
  id_t tile_count;
};


//===--------------------------------------------------------------------===//
// Tile Group factory
//===--------------------------------------------------------------------===//

class TileGroupFactory {
 public:
  TileGroupFactory();
  virtual ~TileGroupFactory();

  static TileGroup *GetTileGroup(oid_t database_id, oid_t table_id, oid_t tile_group_id,
                                 Backend* backend,
                                 const std::vector<catalog::Schema>& schemas,
                                 int tuple_count) {

    TileGroupHeader *tile_header = new TileGroupHeader(backend, tuple_count);

    TileGroup *tile_group = new TileGroup(tile_header, backend, schemas, tuple_count);

    TileGroupFactory::InitCommon(tile_group, database_id, table_id, tile_group_id);

    return tile_group;
  }

 private:

  static void InitCommon(TileGroup *tile_group,
                         oid_t database_id,
                         oid_t table_id,
                         oid_t tile_group_id) {

    tile_group->database_id = database_id;
    tile_group->tile_group_id = tile_group_id;
    tile_group->table_id = table_id;

  }

};


} // End storage namespace
} // End nstore namespace


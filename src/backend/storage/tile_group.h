//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.h
//
// Identification: src/backend/storage/tile_group.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <map>
#include <atomic>
#include <vector>
#include <mutex>
#include <memory>
#include <backend/planner/project_info.h>

#include "backend/common/types.h"
#include "backend/common/printable.h"

namespace peloton {

class VarlenPool;

namespace catalog {
class Manager;
class Schema;
}

namespace planner {
class ProjectInfo;
}

namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group
//===--------------------------------------------------------------------===//

class Tuple;
class Tile;
class TileGroupHeader;
class AbstractTable;
class TileGroupIterator;
class RollbackSegment;

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
class TileGroup : public Printable {
  friend class Tile;
  friend class TileGroupFactory;

  TileGroup() = delete;
  TileGroup(TileGroup const &) = delete;

 public:
  // Tile group constructor
  TileGroup(BackendType backend_type, TileGroupHeader *tile_group_header,
            AbstractTable *table, const std::vector<catalog::Schema> &schemas,
            const column_map_type &column_map, int tuple_count);

  ~TileGroup();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  void ApplyRollbackSegment(char *rb_seg, const oid_t &tuple_slot_id);

  // copy tuple in place.
  void CopyTuple(const Tuple *tuple, const oid_t &tuple_slot_id);

  void CopyTuple(const oid_t &tuple_slot_id, Tuple *tuple);

  // insert tuple at next available slot in tile if a slot exists
  oid_t InsertTuple(const Tuple *tuple);

  // insert tuple at specific tuple slot
  // used by recovery mode
  oid_t InsertTupleFromRecovery(cid_t commit_id, oid_t tuple_slot_id,
                                const Tuple *tuple);

  // insert tuple at specific tuple slot
  // used by recovery mode
  oid_t DeleteTupleFromRecovery(cid_t commit_id, oid_t tuple_slot_id);

  // insert tuple at specific tuple slot
  // used by recovery mode
  oid_t UpdateTupleFromRecovery(cid_t commit_id, oid_t tuple_slot_id,
                                ItemPointer new_location);

  oid_t InsertTupleFromCheckpoint(oid_t tuple_slot_id, const Tuple *tuple,
                                  cid_t commit_id);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

  oid_t GetNextTupleSlot() const;

  // this function is called only when building tile groups for aggregation
  // operations.
  // FIXME: GC has recycled some of the tuples, so this count is not accurate
  oid_t GetActiveTupleCount() const;

  oid_t GetAllocatedTupleCount() const { return num_tuple_slots; }

  TileGroupHeader *GetHeader() const { return tile_group_header; }

  void SetHeader(TileGroupHeader *header) { tile_group_header = header; }

  unsigned int NumTiles() const { return tiles.size(); }

  // Get the tile at given offset in the tile group
  Tile *GetTile(const oid_t tile_itr) const;

  // Get a reference to the tile at the given offset in the tile group
  std::shared_ptr<Tile> GetTileReference(const oid_t tile_offset) const;

  oid_t GetTileId(const oid_t tile_id) const;

  peloton::VarlenPool *GetTilePool(const oid_t tile_id) const;

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

  double GetSchemaDifference(const storage::column_map_type &new_column_map);

  // Sync the contents
  void Sync();

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Catalog information
  oid_t database_id;
  oid_t table_id;
  oid_t tile_group_id;

  // Backend type
  BackendType backend_type;

  // mapping to tile schemas
  std::vector<catalog::Schema> tile_schemas;

  // set of tiles
  std::vector<std::shared_ptr<Tile>> tiles;

  // associated tile group
  TileGroupHeader *tile_group_header;

  // associated table
  AbstractTable *table;  // this design is fantastic!!!

  // number of tuple slots allocated
  oid_t num_tuple_slots;

  // number of tiles
  oid_t tile_count;

  std::mutex tile_group_mutex;

  // column to tile mapping :
  // <column offset> to <tile offset, tile column offset>
  column_map_type column_map;
};

}  // End storage namespace
}  // End peloton namespace

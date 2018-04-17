//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.h
//
// Identification: src/include/storage/tile_group.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "common/item_pointer.h"
#include "common/printable.h"
#include "planner/project_info.h"
#include "storage/layout.h"
#include "type/abstract_pool.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {

namespace catalog {
class Manager;
class Schema;
}

namespace gc {
class GCManager;
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
  friend class gc::GCManager;

  TileGroup() = delete;
  TileGroup(TileGroup const &) = delete;

 public:
  // Tile group constructor
  TileGroup(BackendType backend_type, TileGroupHeader *tile_group_header,
            AbstractTable *table, const std::vector<catalog::Schema> &schemas,
            std::shared_ptr<const Layout> layout, int tuple_count);

  ~TileGroup();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  // copy tuple in place.
  void CopyTuple(const Tuple *tuple, const oid_t &tuple_slot_id);

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
  inline Tile *GetTile(const oid_t tile_offset) const {
    PELOTON_ASSERT(tile_offset < tile_count);
    Tile *tile = tiles[tile_offset].get();
    return tile;
  }

  // Get a reference to the tile at the given offset in the tile group
  std::shared_ptr<Tile> GetTileReference(const oid_t tile_offset) const;

  oid_t GetTileId(const oid_t tile_id) const;

  peloton::type::AbstractPool *GetTilePool(const oid_t tile_id) const;

  oid_t GetTileGroupId() const;

  oid_t GetDatabaseId() const { return database_id; }

  oid_t GetTableId() const { return table_id; }

  AbstractTable *GetAbstractTable() const { return table; }

  void SetTileGroupId(oid_t tile_group_id_) { tile_group_id = tile_group_id_; }

  size_t GetTileCount() const { return tile_count; }

  type::Value GetValue(oid_t tuple_id, oid_t column_id);

  void SetValue(type::Value &value, oid_t tuple_id, oid_t column_id);

  // Sync the contents
  void Sync();

  const storage::Layout& GetLayout() const { return *tile_group_layout_; }

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

  // Refernce to the layout of the TileGroup
  std::shared_ptr<const Layout> tile_group_layout_;
};

}  // namespace storage
}  // namespace peloton

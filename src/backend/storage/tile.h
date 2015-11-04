//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile.h
//
// Identification: src/backend/storage/tile.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group_header.h"

#include <mutex>
#include "backend.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

class TileGroup;
class TupleIterator;
// class TileStats;

/**
 * Represents a Tile.
 *
 * Tiles are only instantiated via TileFactory.
 *
 * NOTE: MVCC is implemented on the shared TileGroupHeader.
 */
class Tile {
  friend class TileFactory;
  friend class TupleIterator;
  friend class TileGroupHeader;

  Tile() = delete;
  Tile(Tile const &) = delete;

 public:
  // Tile creator
  Tile(TileGroupHeader *tile_header,
       const catalog::Schema &tuple_schema,
       TileGroup *tile_group,
       int tuple_count);

  virtual ~Tile();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  /**
   * Insert tuple at slot
   * NOTE : No checks, must be at valid slot.
   */
  void InsertTuple(const oid_t tuple_slot_id, Tuple *tuple);

  // allocated tuple slots
  oid_t GetAllocatedTupleCount() const { return num_tuple_slots; }

  // active tuple slots
  inline virtual oid_t GetActiveTupleCount() const {
    // For normal tiles
    if (tile_group_header != nullptr) {
      return tile_group_header->GetNextTupleSlot();
    }
    // For temp tiles
    else {
      return num_tuple_slots;
    }
  }

  int GetTupleOffset(const char *tuple_address) const;

  int GetColumnOffset(const std::string &name) const;

  /**
   * Returns tuple present at slot
   * NOTE : No checks, must be at valid slot and must exist.
   */
  Tuple *GetTuple(const oid_t tuple_slot_id);

  /**
   * Returns value present at slot
   * NOTE : No checks, must be at valid slot and must exist.
   */
  Value GetValue(const oid_t tuple_slot_id, const oid_t column_id);

  // Faster way to access value
  // By amortizing schema lookups
  Value GetValueFast(const oid_t tuple_slot_id, const size_t column_offset,
                     const ValueType column_type, const bool is_inlined);

  void SetValue(Value value, const oid_t tuple_slot_id, const oid_t column_id);

  // Faster way to set values
  // By amortizing schema lookups
  void SetValueFast(Value value, const oid_t tuple_slot_id,
                    const size_t column_offset,
                    const bool is_inlined,
                    const size_t column_length);

  // Get tuple at location
  static Tuple *GetTuple(catalog::Manager *catalog,
                         const ItemPointer *tuple_location);

  // Copy current tile in given backend and return new tile
  Tile *CopyTile();

  //===--------------------------------------------------------------------===//
  // Size Stats
  //===--------------------------------------------------------------------===//

  // Only inlined data
  uint32_t GetInlinedSize() const { return tile_size; }

  int64_t GetUninlinedDataSize() const { return uninlined_data_size; }

  // Both inlined and uninlined data
  uint32_t GetSize() const { return tile_size + uninlined_data_size; }

  //===--------------------------------------------------------------------===//
  // Columns
  //===--------------------------------------------------------------------===//

  const catalog::Schema *GetSchema() const { return &schema; };

  const std::string GetColumnName(const oid_t column_index) const {
    return schema.GetColumn(column_index).column_name;
  }

  inline oid_t GetColumnCount() const { return column_count; };

  inline TileGroupHeader *GetHeader() const { return tile_group_header; }

  inline TileGroup *GetTileGroup() const { return tile_group; }

  oid_t GetTileId() const { return tile_id; }

  // Compare two tiles
  bool operator==(const Tile &other) const;
  bool operator!=(const Tile &other) const;

  TupleIterator GetIterator();

  // Get a string representation of this tile
  friend std::ostream &operator<<(std::ostream &os, const Tile &tile);

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//

  bool SerializeTo(SerializeOutput &output, oid_t num_tuples);
  bool SerializeHeaderTo(SerializeOutput &output);
  bool SerializeTuplesTo(SerializeOutput &output, Tuple *tuples,
                         int num_tuples);

  void DeserializeTuplesFrom(SerializeInputBE &serialize_in,
                             VarlenPool *pool = nullptr);
  void DeserializeTuplesFromWithoutHeader(SerializeInputBE &input,
                                          VarlenPool *pool = nullptr);

  VarlenPool *GetPool() { return (pool); }

  char *GetTupleLocation(const oid_t tuple_slot_id) const;

  // maintain reference counts for
  void IncrementRefCount();

  void DecrementRefCount();

  size_t GetRefCount() const;

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Catalog information
  oid_t database_id;
  oid_t table_id;
  oid_t tile_group_id;
  oid_t tile_id;

  // tile schema
  catalog::Schema schema;

  // set of fixed-length tuple slots
  char *data;

  // relevant tile group
  TileGroup *tile_group;

  // storage pool for uninlined data
  VarlenPool *pool;

  // number of tuple slots allocated
  oid_t num_tuple_slots;

  // number of columns
  oid_t column_count;

  // length of tile tuple
  size_t tuple_length;

  // space occupied by inlined data (tile size)
  size_t tile_size;

  // space occupied by uninlined data
  size_t uninlined_data_size;

  // Used for serialization/deserialization
  char *column_header;

  oid_t column_header_size;

  /**
   * NOTE : Tiles don't keep track of number of occupied slots.
   * This is maintained by shared Tile Header.
   */
  TileGroupHeader *tile_group_header;

  // references
  std::atomic<size_t> ref_count;

};

// Returns a pointer to the tuple requested. No checks are done that the index
// is valid.
inline char *Tile::GetTupleLocation(const oid_t tuple_slot_id) const {
  char *tuple_location = data + (tuple_slot_id * tuple_length);

  return tuple_location;
}

// Finds index of tuple for a given tuple address.
// Returns -1 if no matching tuple was found
inline int Tile::GetTupleOffset(const char *tuple_address) const {
  // check if address within tile bounds
  if ((tuple_address < data) || (tuple_address >= (data + tile_size)))
    return -1;

  int tuple_id = 0;

  // check if address is at an offset that is an integral multiple of tuple
  // length
  tuple_id = (tuple_address - data) / tuple_length;

  if (tuple_id * tuple_length + data == tuple_address) return tuple_id;

  return -1;
}

//===--------------------------------------------------------------------===//
// Tile factory
//===--------------------------------------------------------------------===//

class TileFactory {
 public:
  TileFactory();
  virtual ~TileFactory();

  // Creates tile that is not attached to a tile group.
  // For use in the executor.
  static Tile *GetTempTile(const catalog::Schema &schema, int tuple_count) {
    // These temporary tiles don't belong to any tile group.
    TileGroupHeader *header = nullptr;
    TileGroup *tile_group = nullptr;

    Tile *tile = GetTile(INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
                         header, schema, tile_group, tuple_count);

    return tile;
  }

  static Tile *GetTile(oid_t database_id, oid_t table_id, oid_t tile_group_id,
                       oid_t tile_id, TileGroupHeader *tile_header,
                       const catalog::Schema &schema,
                       TileGroup *tile_group, int tuple_count) {
    Tile *tile =
        new Tile(tile_header, schema, tile_group, tuple_count);

    TileFactory::InitCommon(tile, database_id, table_id, tile_group_id, tile_id,
                            schema);

    return tile;
  }

 private:
  static void InitCommon(Tile *tile, oid_t database_id, oid_t table_id,
                         oid_t tile_group_id, oid_t tile_id,
                         const catalog::Schema &schema) {
    tile->database_id = database_id;
    tile->table_id = table_id;
    tile->tile_group_id = tile_group_id;
    tile->tile_id = tile_id;
    tile->schema = schema;
  }
};

}  // End storage namespace
}  // End peloton namespace

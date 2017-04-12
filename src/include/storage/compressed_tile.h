//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_tile.h
//
// Identification: src/include/storage/compressed_tile.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/item_pointer.h"
#include "common/printable.h"
#include "type/abstract_pool.h"
#include "type/serializeio.h"
#include "type/serializer.h"
#include "storage/tile.h"

namespace peloton {

namespace gc {
class GCManager;
}

namespace storage {

//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

class Tuple;
class TileGroup;
class TileGroupHeader;
class TupleIterator;
class Tile;

/**
 * Represents a Tile.
 *
 * Tiles are only instantiated via TileFactory.
 *
 * NOTE: MVCC is implemented on the shared TileGroupHeader.
 */
class CompressedTile : public Tile {
  CompressedTile(CompressedTile const &) = delete;
  CompressedTile() = delete;

 public:
  // Constructor
  CompressedTile(BackendType backend_type, TileGroupHeader *tile_header,
                 const catalog::Schema &tuple_schema, TileGroup *tile_group,
                 int tuple_count);

  // Destructor
  ~CompressedTile();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  void CompressTile(Tile *tile);

  std::vector<type::Value> CompressColumn(Tile *tile, oid_t column_id,
                                          type::Type::TypeId compression_type);

  void InsertTuple(const oid_t tuple_offset, Tuple *tuple);

  type::Value GetValue(const oid_t tuple_offset, const oid_t column_id);

  type::Value GetValueFast(const oid_t tuple_offset, const size_t column_offset,
                           const type::Type::TypeId column_type,
                           const bool is_inlined);

  void SetValue(const type::Value &value, const oid_t tuple_offset,
                const oid_t column_id);

  void SetValueFast(const type::Value &value, const oid_t tuple_offset,
                    const size_t column_offset, const bool is_inlined,
                    const size_t column_length);

  //===--------------------------------------------------------------------===//
  // Utility Functions
  //===--------------------------------------------------------------------===//

  bool IsCompressed() { return is_compressed; }

  inline type::Value GetBaseValue(type::Value old_value,
                                  type::Value new_value) {
    return old_value.Subtract(new_value).CastAs(old_value.GetTypeId());
  }

  inline type::Type::TypeId GetCompressedType(type::Value new_value) {
    return new_value.GetTypeId();
  }

  inline void SetCompressedMapValue(oid_t column_id, type::Type::TypeId type_id,
                                    type::Value base_value) {
    compressed_column_map[column_id] = std::make_pair(type_id, base_value);
  }

  inline type::Value GetBaseValue(oid_t column_id) {
    if (compressed_column_map.find(column_id) != compressed_column_map.end()) {
      return compressed_column_map[column_id].second;
    }
    return type::Value();
  }

  inline type::Type::TypeId GetCompressedType(oid_t column_id) {
    if (compressed_column_map.find(column_id) != compressed_column_map.end()) {
      return compressed_column_map[column_id].first;
    }
    return type::Type::INVALID;
  }

  inline type::Value GetUncompressedValue(oid_t column_id,
                                          type::Value compressed_value) {
    if (compressed_column_map.find(column_id) != compressed_column_map.end()) {
      type::Value base_value = GetBaseValue(column_id);
      return base_value.Add(compressed_value).CastAs(base_value.GetTypeId());
    }

    return type::Value();
  }

  oid_t GetColumnFromOffsest(const size_t column_offset) {
    oid_t i;
    PL_ASSERT(column_offset < schema.GetLength());
    for (i = 0; i < column_count; i++) {
      if (schema.GetOffset(i) == column_offset) {
        break;
      }
    }
    return i;
  }

 protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  bool is_compressed;
  oid_t compressed_columns_count;
  int tuple_count;

  std::map<oid_t, std::pair<type::Type::TypeId, type::Value>>
      compressed_column_map;
};
}
}

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile.h
//
// Identification: src/include/storage/tile.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <vector>
#include <map>
#include <string>
#include <set>

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/item_pointer.h"
#include "common/printable.h"
#include "type/abstract_pool.h"
#include "type/serializeio.h"
#include "type/serializer.h"
#include "tile.h"

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
class DictEncodedTile : public Tile {
  friend class TileFactory;
  friend class TupleIterator;
  friend class TileGroupHeader;
  friend class gc::GCManager;

  DictEncodedTile() = delete;
  DictEncodedTile(DictEncodedTile const &) = delete;

 public:
  // Tile creator
  DictEncodedTile(BackendType backend_type, TileGroupHeader *tile_header,
       const catalog::Schema &tuple_schema, TileGroup *tile_group,
       int tuple_count);

  virtual ~DictEncodedTile();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  /**
   * Insert tuple at slot
   * NOTE : No checks, must be at valid slot.
   */
//  void InsertTuple(const oid_t tuple_offset, Tuple *tuple);

  const catalog::Schema *GetSchema() const override { return &original_schema; }  ;

  /**
   * Returns value present at slot
   */
  type::Value GetValue(const oid_t tuple_offset, const oid_t column_id) override ;

  /*
   * Faster way to get value
   * By amortizing schema lookups
   */
  type::Value GetValueFast(const oid_t tuple_offset, const size_t column_offset,
                           const type::TypeId column_type,
                           const bool is_inlined) override ;

  /**
   * Sets value at tuple slot.
   */
   // same as tile, no need to override
//  void SetValue(const type::Value &value, const oid_t tuple_offset,
//                const oid_t column_id);

  /*
   * Faster way to set value
   * By amortizing schema lookups
   */
   // also same as tile
//  void SetValueFast(const type::Value &value, const oid_t tuple_offset,
//                    const size_t column_offset, const bool is_inlined,
//                    const size_t column_length);

  // Copy current tile in given backend and return new tile
  Tile *CopyTile(BackendType backend_type);

  //===--------------------------------------------------------------------===//
  // Dictionary Encoding
  //===--------------------------------------------------------------------===//

	inline bool GetDictEncoded() const { return is_dict_encoded; }

	// only encode varchar, assume this tail is full
	void DictEncode(Tile *tile);

  Tile* DictDecode();

 protected:
  // is dictionary encoded
  bool is_dict_encoded;

  std::vector<type::Value> element_array;

  std::map<type::Value, uint8_t, type::Value::equal_to> dict;

  std::set<oid_t> dict_encoded_columns;

  catalog::Schema original_schema;

  std::map<size_t, oid_t> original_schema_offsets;
};

}  // namespace storage
}  // namespace peloton

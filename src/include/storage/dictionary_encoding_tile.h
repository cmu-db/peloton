//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dictionary_encoding_tile.h
//
// Identification: src/include/storage/dictionary_encoding_tile.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <vector>
#include <string>
#include <set>
#include <unordered_map>

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
	// this function is used before GetValueFast which use
	// original schema, so return original schema
  //  const catalog::Schema *GetSchema() const override { return &original_schema; }  ;

  /**
   * Returns value present at slot
   */
  type::Value GetValue(const oid_t tuple_offset, const oid_t column_id) override ;

  /*
   * Faster way to get value
   * By amortizing schema lookups
   */
   // since we know that to use this function, we first get schema and extract
   // info from schema then use this function, so we assume the column offset
   // is the original column offset, therefore transform needed
  type::Value GetValueFast(const oid_t tuple_offset, const size_t column_offset,
                           const type::TypeId column_type,
                           const bool is_inlined) override ;

  //===--------------------------------------------------------------------===//
  // Dictionary Encoding
  //===--------------------------------------------------------------------===//

  //	inline bool IsDictEncoded() const { return is_dict_encoded; }

	// given a tile, encode this tile in current tile
	// when initializing this encoded tile, use original tile's schema
	void DictEncode(Tile *tile) override ;

	// decode tile and return a new tile that contain the decoded data
  Tile* DictDecode() override ;

	inline bool IsColumnEncoded(oid_t column_offset) const override {
		return dict_encoded_columns.find(column_offset) != dict_encoded_columns.end();
	}

	inline char *GetElementArray(oid_t column_offset) override {
		if (IsColumnEncoded(column_offset)) {
			return varlen_val_ptrs;
		}
		return nullptr;
	}

 protected:

	// the idx-string mapping
  std::vector<type::Value> element_array;
	// the string-idx mapping
  std::unordered_map<type::Value, uint8_t, type::Value::hash, type::Value::compress_equal_to> dict;
	// columns being encoded
  std::map<oid_t, oid_t> dict_encoded_columns;
//	std::set<oid_t> dict_encoded_columns;
	// original schema
  catalog::Schema original_schema;
	// original column offset
  std::map<size_t, oid_t> original_schema_offsets;
  // element ptr
  char *varlen_val_ptrs;
};

}  // namespace storage
}  // namespace peloton

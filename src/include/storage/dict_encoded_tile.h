//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dictionary_encoding_tile.h
//
// Identification: src/include/storage/dictionary_encoding_tile.h
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
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
// DictEncodedTile
//===--------------------------------------------------------------------===//

class Tuple;
class TileGroup;
class TileGroupHeader;
class TupleIterator;
class Tile;
/**
 * Represents a Dictionary Encoded Tile.
 *
 * DictEncodedTiles are only instantiated via TileGroup.
 *
 * DictEncodedTile is subclass of Tile.
 * 
 * The DictEncodedTile can only be read. The only two operations it supports
 * are GetValue and GetValueFast. 
 * 
 * The ColumnIsEncoded and GetElementArray is used in Codegen. They support
 * queries on encoded data. GetElementArray gives the query the index the query needs
 * to decode the data.
 */
class DictEncodedTile : public Tile {
  friend class TileFactory;
  friend class TupleIterator;
  friend class TileGroupHeader;
  friend class gc::GCManager;

  DictEncodedTile() = delete;
  DictEncodedTile(DictEncodedTile const &) = delete;

 public:
  // DictEncodedTile creator
  DictEncodedTile(BackendType backend_type, TileGroupHeader *tile_header,
       const catalog::Schema &tuple_schema, TileGroup *tile_group,
       int tuple_count);

  virtual ~DictEncodedTile();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  /**
   * Returns original value after decoded present at slot
   */
  type::Value GetValue(const oid_t tuple_offset, const oid_t column_id) override ;

  /*
   * Faster way to get original value after decoded
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


	// given a tile, encode this tile in current tile
	// when initializing this encoded tile, use original tile's schema
	void DictEncode(Tile *tile) override ;

	// decode tile and return a new tile that contain the decoded data
  Tile* DictDecode() override ;

  // check whether the column is encoded or not
	inline bool ColumnIsEncoded(oid_t column_offset) const override {
		return dict_encoded_columns.find(column_offset) != dict_encoded_columns.end();
	}
  // get the idx-string mapping array pointer
	inline char *GetElementArray(oid_t column_offset) override {
		if (ColumnIsEncoded(column_offset)) {
			return varlen_val_ptrs;
		}
		return nullptr;
	}

 protected:
	// the string-idx mapping
  std::unordered_map<type::Value, uint32_t, type::Value::hash, type::Value::compress_equal_to> dict;
	// columns being encoded
  std::map<oid_t, oid_t> dict_encoded_columns;
	// original schema
  catalog::Schema original_schema;
	// original column offset
  std::map<size_t, oid_t> original_schema_offsets;
  // element ptr, the idx-string mapping
  char *varlen_val_ptrs;
};

}  // namespace storage
}  // namespace peloton

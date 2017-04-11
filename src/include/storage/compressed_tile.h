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

	std::vector<type::Value> CompressColumn(Tile* tile, oid_t column_id, 
																type::Type::TypeId compression_type);


  //===--------------------------------------------------------------------===//
  // Utility Functions
  //===--------------------------------------------------------------------===//

	bool IsCompressed () {
		return is_compressed;
	}

protected:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

	bool is_compressed;
	oid_t compressed_columns_count;
	int tuple_count;
};
}
}

/*-------------------------------------------------------------------------
 *
 * static_tile.h
 * fixed length tiles (no MVCC)
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/static_tile.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/tile.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Static Tile
//===--------------------------------------------------------------------===//

class TileIterator;

/**
 * Represents a Static StaticTile.
 *
 * Fixed # of slots, all are active
 * No tile group header
 * NOTE: No MVCC is performed for these tiles.
 *
 * StaticTiles are only instantiated via TileFactory.
 */
class StaticTile : public Tile {
	friend class TileFactory;
	friend class TileIterator;

	StaticTile() = delete;
	StaticTile(StaticTile const&) = delete;

public:

	// StaticTile creator
	StaticTile(Backend* backend, catalog::Schema *tuple_schema,
			int tuple_count, const std::vector<std::string>& column_names, bool own_schema) :
				Tile(nullptr, backend, tuple_schema, tuple_count, column_names, own_schema) {
	}

	virtual ~StaticTile(){}

	// Active tuple slots
	inline id_t GetActiveTupleCount() const {
		return num_tuple_slots;
	}

};


} // End storage namespace
} // End nstore namespace

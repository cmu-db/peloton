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

#include "storage/tile.h"
#include "storage/tile_header.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group
//===--------------------------------------------------------------------===//

/**
 * Represents a group of tiles logically horizontally contiguous.
 *
 * < <Tile 1> <Tile 2> .. <Tile n> >
 *
 */
class TileGroup {
	friend class Tile;

public:
	virtual ~TileGroup(){}

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	/// MVCC in tile header

	bool InsertTuple(Tuple &source);

	int64_t GetOccupiedSize() const {
		return next_tuple_slot;
	}

	// active tuples in tile
	int64_t GetActiveTupleCount() const {
		return next_tuple_slot;
	}

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	/// set of tiles
	std::vector<Tile*> tiles;

	TileHeader* tile_header;

	/// Catalog information
	Oid tile_group_id;
	Oid table_id;
	Oid database_id;

	// next free slot iterator
	id_t next_tuple_slot;
};

} // End storage namespace
} // End nstore namespace


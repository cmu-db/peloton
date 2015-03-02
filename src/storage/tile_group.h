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

	/// MVCC on the primary tile

	bool InsertTuple(Tuple &source);
	bool UpdateTuple(Tuple &source, Tuple &target, bool update_indexes);
	bool DeleteTuple(Tuple &tuple, bool free_uninlined_columns);

	void DeleteAllTuples(bool freeAllocatedStrings);

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	/// set of tiles
	std::vector<Tile*> tiles;

	/// Catalog information
	Oid tile_group_id;
	Oid table_id;
	Oid database_id;

};

} // End storage namespace
} // End nstore namespace


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
#include "storage/tile_group_header.h"
#include "storage/tile_group_iterator.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group
//===--------------------------------------------------------------------===//

class TileGroupIterator;

/**
 * Represents a group of tiles logically horizontally contiguous.
 *
 * < <Tile 1> <Tile 2> .. <Tile n> >
 *
 * Look at TileGroupHeader for MVCC implementation.
 *
 */
class TileGroup {
	friend class Tile;

public:
	virtual ~TileGroup(){}

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	bool InsertTuple(Tuple &source);

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	// set of tiles
	std::vector<Tile*> tiles;

	TileGroupHeader* tile_header;

	// Catalog information
	id_t tile_group_id;
	id_t table_id;
	id_t database_id;

};

} // End storage namespace
} // End nstore namespace


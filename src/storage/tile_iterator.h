/*-------------------------------------------------------------------------
*
* table_iterator.h
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/table_iterator.h
*
*-------------------------------------------------------------------------
*/

#pragma once

#include <iostream>

#include "common/iterator.h"
#include "storage/tile.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Iterator
//===--------------------------------------------------------------------===//

/**
 * Iterator for table which neglects deleted tuples.
 * TileIterator is a small and copiable object.
 * You can copy it, not passing a pointer of it.
 */
class TileIterator : public Iterator<Tuple> {
	TileIterator() = delete;

public:
	TileIterator(const Tile& tile) :
		tile(tile),
		tile_itr(tile.data) {
	}

	TileIterator(const TileIterator& other) :
		tile(other.tile),
		tile_itr(other.tile_itr) {
	}

	/**
	 * Updates the given tuple so that it points to the next tuple in the table.
	 * @param out the tuple will point to the retrieved tuple if this method returns true.
	 * @return true if succeeded. false if no more active tuple is there.
	 */
	bool Next(Tuple &out);

	bool HasNext();

	const char* GetLocation() const;

private:
	bool ContinuationPredicate();

	/// Base tile
	const Tile& tile;

	/// Iterator over tile data
	char *tile_itr;
};

bool TileIterator::ContinuationPredicate() {
	/// Scan until within the tile
	return (tile_itr < (tile.data + tile.tile_size));
}

bool TileIterator::HasNext() {
	return ContinuationPredicate();
}

bool TileIterator::Next(Tuple &out) {
	while (ContinuationPredicate()) {
		out.Move(tile_itr);

		tile_itr += tile.tuple_length;

		/// Return this tuple only when this tuple is not marked as deleted.
		//if (out.IsVisible()) {
		return true;
		//}
	}

	return false;
}

const char* TileIterator::GetLocation() const {
	return tile_itr;
}


} // End storage namespace
} // End nstore namespace




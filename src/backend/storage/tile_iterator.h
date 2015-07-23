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

#include "backend/common/iterator.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Iterator
//===--------------------------------------------------------------------===//

/**
 * Iterator for tile which goes over all active tuples.
 *
 **/
class TileIterator : public Iterator<Tuple> {
	TileIterator() = delete;

public:
	TileIterator(const Tile* tile) :
		data(tile->data),
		tile(tile),
		tile_itr(0),
		tuple_length(tile->tuple_length) {

		tile_group_header = tile->tile_group_header;

	}

	TileIterator(const TileIterator& other) :
		data(other.data),
		tile(other.tile),
		tile_group_header(other.tile_group_header),
		tile_itr(other.tile_itr),
		tuple_length(other.tuple_length) {
	}

	/**
	 * Updates the given tuple so that it points to the next tuple in the table.
	 * @return true if succeeded. false if no more tuples are there.
	 */
	bool Next(Tuple &out) {
    if(HasNext()) {

      out.Move(data + (tile_itr * tuple_length));
      tile_itr++;

      return true;
    }

    return false;
  }

	bool HasNext() {
    return (tile_itr < tile->GetActiveTupleCount());
  }

	void IncrementIterator() {
		tile_itr++;
	}


  oid_t GetLocation() const {
    return tile_itr;
  }


private:
	// Base tile data
	char* data;

	const Tile * tile;

	const TileGroupHeader *tile_group_header;

	// Iterator over tile data
	oid_t tile_itr;

	oid_t tuple_length;
};

} // End storage namespace
} // End peloton namespace

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

#include "common/iterator.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"

namespace nstore {
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
		tile_itr(0),
		tuple_length(tile->tuple_length),
		is_static(false),
		static_tuple_count(0) {

		tile_group_header = tile->tile_group_header;

		// determine if tile is static
		if(tile_group_header == nullptr) {
			is_static = true;
			static_tuple_count = tile->GetAllocatedTupleCount();
		}
	}

	TileIterator(const TileIterator& other) :
		data(other.data),
		tile_group_header(other.tile_group_header),
		tile_itr(other.tile_itr),
		tuple_length(other.tuple_length),
		is_static(other.is_static),
		static_tuple_count(other.static_tuple_count) {
	}

	/**
	 * Updates the given tuple so that it points to the next tuple in the table.
	 * @return true if succeeded. false if no more tuples are there.
	 */
	bool Next(Tuple &out);

	bool HasNext();

	id_t GetLocation() const;

private:
	// Base tile data
	char* data;

	const TileGroupHeader *tile_group_header;

	// Iterator over tile data
	id_t tile_itr;

	id_t tuple_length;

	// Is it static ?
	bool is_static;

	// static tuple count
	id_t static_tuple_count;
};

bool TileIterator::HasNext() {

	// Scan until active tuples
	if(is_static == false) {
		//std::cout << "Tile Itr :: " << tile_itr << " Active   :: "
		//		<< tile_group_header->GetActiveTupleCount() << "\n";

		return (tile_itr < tile_group_header->GetActiveTupleCount());
	}
	else {
		//std::cout << "Tile Itr :: " << tile_itr << " Active   :: "
		//		<< static_tuple_count << "\n";

		return (tile_itr < static_tuple_count);
	}
}

bool TileIterator::Next(Tuple &out) {
	if(HasNext()) {
		out.Move(data + (tile_itr * tuple_length));
		tile_itr++;

		return true;
	}

	return false;
}

id_t TileIterator::GetLocation() const {
	return tile_itr;
}


} // End storage namespace
} // End nstore namespace




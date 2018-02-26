//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.cpp
//
// Identification: src/storage/layout.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/layout.h"

namespace peloton {
namespace storage {

// Sets the tile id and column id w.r.t that tile corresponding to
// the specified tile group column id.
void Layout::LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                              oid_t &tile_column_offset) const {

	// Ensure that the column_offset is not out of bound
	PL_ASSERT(num_columns_ > column_offset);

	// For row store layout, tile id is always 0 and the tile
	// column_id and tile_group column_id is the same.
	if (layout_id_ == ROW_STORE_OID) {
		tile_offset = 0;
		tile_column_offset = column_offset;
		return;
	}

	// For other layouts, fetch the layout and
	// get the entry in the column map
	auto entry = tile_group_layout_.at(column_offset);
	tile_offset = entry.first;
	tile_column_offset = entry.second;
}


} // namespace storage
} // namespace peloton
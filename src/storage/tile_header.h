/*-------------------------------------------------------------------------
 *
 * tile_header.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_header.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <iostream>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Header
//===--------------------------------------------------------------------===//

class TileHeader {
	TileHeader() = delete;

public:
	TileHeader(Backend* backend, int tuple_count) {

	}

	TileHeader(const TileHeader& other) {
	}

private:

};


} // End storage namespace
} // End nstore namespace




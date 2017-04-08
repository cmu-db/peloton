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

namespace peloton {

namespace gc {
class GCManager;
}

namespace storage {

//===--------------------------------------------------------------------===//
// Compressed Tile
//===--------------------------------------------------------------------===//

class Tuple;
class TileGroup;
class TileGroupHeader;
class TupleIterator;
class Tile

class CompressedTile : public Tile {

	//
	bool IsCompressed() const { return true; }

	

}; // end Class CompressedTile
} // end namespace storage
} // end namespace peloton
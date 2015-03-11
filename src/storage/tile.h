/*-------------------------------------------------------------------------
 *
 * tile.h
 * the base class for all tiles
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "../catalog/schema.h"
#include "storage/tuple.h"
#include "storage/tile.h"
#include "storage/backend.h"

#include <mutex>

//#include "storage/tile_stats.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

class TileIterator;

/**
 * Represents a Tile.
 *
 *	 next_slot : location of next empty slot (atomic).
 *
 *   temp_tuple: when a transaction is inserting a new tuple, this tuple
 *     object is used as a reusable value-holder for the new tuple.
 *     In this way, we don't have to allocate a temporary tuple each time.
 *
 * Tiles are only instantiated via TileFactory.
 */
class Tile {
	friend class TileIterator;
	friend class TileFactory;
	//friend class TileStats;

	Tile() = delete;
	Tile(Tile const&) = delete;

public:

	// Tile creator
	Tile(Backend* backend, catalog::Schema *tuple_schema, int tuple_count,
			const std::vector<std::string>& column_names, bool own_schema);

	~Tile();

	//===--------------------------------------------------------------------===//
	// Operations and stats
	//===--------------------------------------------------------------------===//

	bool InsertTuple(const id_t tuple_slot_id, Tuple *tuple);

	Tuple& TempTuple() {
		//assert(!temp_tuple.IsNull());
		return temp_tuple;
	}

	// allocated tuples in tile
	int64_t GetAllocatedTupleCount() const {
		return max_tuple_slots;
	}

	// active tuples in tile
	int64_t GetActiveTupleCount() const {
		return next_tuple_slot;
	}

	int GetTupleOffset(const char *tuple_address) const;

	int GetColumnOffset(const std::string &name) const;

	//===--------------------------------------------------------------------===//
	// Size Stats
	//===--------------------------------------------------------------------===//

	// Only inlined data
	uint32_t GetInlinedSize() const {
		return tile_size ;
	}

	int64_t GetUninlinedDataSize() const {
		return uninlined_data_size;
	}

	// Both inlined and uninlined data
	uint32_t GetSize() const {
		return tile_size + uninlined_data_size ;
	}

	int64_t GetOccupiedSize() const {
		return next_tuple_slot * temp_tuple.GetLength();
	}

	//===--------------------------------------------------------------------===//
	// Columns
	//===--------------------------------------------------------------------===//

	const catalog::Schema* GetSchema() const {
		return schema;
	};

	const std::string& GetColumnName(int index) const {
		return column_names[index];
	}

	int GetColumnCount() const {
		return column_count;
	};

	const std::vector<std::string> GetColumns() const {
		return column_names;
	}

	// Compare two tiles
	bool operator== (const Tile &other) const;
	bool operator!= (const Tile &other) const;

	TileIterator GetIterator();

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	// Get a string representation of this tile
	friend std::ostream& operator<<(std::ostream& os, const Tile& tile);

	//===--------------------------------------------------------------------===//
	// Serialization/Deserialization
	//===--------------------------------------------------------------------===//

	bool SerializeTo(SerializeOutput &output);
	bool SerializeHeaderTo(SerializeOutput &output);
	bool SerializeTuplesTo(SerializeOutput &output, Tuple *tuples, int num_tuples);

	void DeserializeTuplesFrom(SerializeInput &serialize_in, Pool *pool = NULL);
	void DeserializeTuplesFromWithoutHeader(SerializeInput &input, Pool *pool = NULL);

	Pool* GetPool(){
		return (pool);
	}

	char *GetTupleLocation(const id_t tuple_slot_id) const;

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	// backend
	Backend *backend;

	// set of fixed-length tuple slots
	char *data;

	// storage pool for uninlined data
	Pool *pool;

	// column header
	std::vector<std::string> column_names;

	// reusable temp tuple
	Tuple temp_tuple;

	// tile schema
	catalog::Schema *schema;

	// do we own the schema ?
	bool own_schema;

	id_t max_tuple_slots;

	// number of columns
	id_t column_count;

	// length of tile tuple
	size_t tuple_length;

	// space occupied by inlined data (tile size)
	size_t tile_size;

	// space occupied by uninlined data
	size_t uninlined_data_size;

	// Catalog information
	id_t tile_id;
	id_t tile_group_id;
	id_t table_id;
	id_t database_id;

	// Used for serialization/deserialization
	char *column_header;

	id_t column_header_size;

	// next free slot iterator
	id_t next_tuple_slot;
};

// Returns a pointer to the tuple requested. No checks are done that the index is valid.
inline char* Tile::GetTupleLocation(const id_t tuple_slot_id) const {
	char *tuple_location = data + ((tuple_slot_id % max_tuple_slots) * tuple_length);

	return tuple_location;
}

// Finds index of tuple for a given tuple address.
// Returns -1 if no matching tuple was found
inline int Tile::GetTupleOffset(const char* tuple_address) const{

	// check if address within tile bounds
	if((tuple_address < data) || (tuple_address >= (data + tile_size)))
		return -1;

	int tuple_id = 0;

	// check if address is at an offset that is an integral multiple of tuple length
	tuple_id = (tuple_address - data)/tuple_length;

	if(tuple_id * tuple_length + data == tuple_address)
		return tuple_id;

	return -1;
}

} // End storage namespace
} // End nstore namespace

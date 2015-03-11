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

#include "catalog/schema.h"
#include "storage/backend.h"
#include "storage/tile_header.h"
#include "storage/tuple.h"
#include "storage/vm_backend.h"

#include <mutex>

//#include "storage/tile_stats.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

class TileIterator;
//class TileStats;

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
	friend class TileHeader;


	Tile() = delete;
	Tile(Tile const&) = delete;

public:

	// Tile creator
	Tile(TileHeader* tile_header, Backend* backend, catalog::Schema *tuple_schema,
			int tuple_count, const std::vector<std::string>& column_names, bool own_schema);

	~Tile();

	//===--------------------------------------------------------------------===//
	// Operations and stats
	//===--------------------------------------------------------------------===//

	bool InsertTuple(const id_t tuple_slot_id, Tuple *tuple);

	Tuple& TempTuple() {
		//assert(!temp_tuple.IsNull());
		return temp_tuple;
	}

	// allocated tuple slots
	int64_t GetAllocatedTupleCount() const {
		return num_tuple_slots;
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

	bool SerializeTo(SerializeOutput &output, id_t num_tuples);
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

	id_t num_tuple_slots;

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

	/**
	 * NOTE : Tiles don't keep track of number of occupied slots.
	 * This is maintained by shared Tile Header.
	 */
	TileHeader *tile_header;
};

// Returns a pointer to the tuple requested. No checks are done that the index is valid.
inline char* Tile::GetTupleLocation(const id_t tuple_slot_id) const {
	char *tuple_location = data + ((tuple_slot_id % num_tuple_slots) * tuple_length);

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


//===--------------------------------------------------------------------===//
// Tile factory
//===--------------------------------------------------------------------===//

class TileFactory {
public:
	TileFactory();
	virtual ~TileFactory();

	static Tile *GetTile(catalog::Schema* schema, int tuple_count,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema){

		Backend* backend = new storage::VMBackend();

		return TileFactory::GetTile(INVALID_ID, INVALID_ID, INVALID_ID, INVALID_ID,
				NULL, schema, backend, tuple_count,
				column_names, owns_tuple_schema);
	}

	static Tile *GetTile(id_t database_id, id_t table_id, id_t tile_group_id, id_t tile_id,
			TileHeader* tile_header, catalog::Schema* schema, Backend* backend,
			int tuple_count,
			const std::vector<std::string>& column_names,
			const bool owns_tuple_schema) {

		// create tile header if passed one is NULL
		if(tile_header == NULL)
			tile_header = new TileHeader(backend, tuple_count);

		Tile *tile = new Tile(tile_header, backend, schema, tuple_count, column_names,
				owns_tuple_schema);

		TileFactory::InitCommon(tile, database_id, table_id, tile_group_id, tile_id,
				schema, column_names, owns_tuple_schema);

		// initialize tile stats

		return tile;
	}

private:

	static void InitCommon(Tile *tile, id_t database_id, id_t table_id, id_t tile_id,
			id_t tile_group_id, catalog::Schema* schema, const std::vector<std::string>& column_names,
			const bool owns_tuple_schema) {

		tile->database_id = database_id;
		tile->tile_group_id = tile_group_id;
		tile->table_id = table_id;
		tile->tile_id = tile_id;

		tile->schema = schema;
		tile->column_names = column_names;
		tile->own_schema = owns_tuple_schema;

	}

};

} // End storage namespace
} // End nstore namespace

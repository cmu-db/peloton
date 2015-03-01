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

//#include "storage/tile_stats.h"

namespace nstore {
namespace storage {

// type name, offset, name length
const size_t COLUMN_DESCRIPTOR_SIZE = 1 + 4 + 4;

const int MAX_TEMP_TILE_MEMORY = 1024 * 1024 * 100; // 100 MB

//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

/**
 * Represents a tile which might or might not be a temporary tile.
 * Both TempTile and PersistentTile derive from this class.
 *
 *   free_tuples: a linked list of free (unused) tuples. The data contains
 *     all tuples, including deleted tuples. deleted tuples in this linked
 *     list are reused on next insertion.
 *   temp_tuple: when a transaction is inserting a new tuple, this tuple
 *     object is used as a reusable value-holder for the new tuple.
 *     In this way, we don't have to allocate a temporary tuple each time.
 *
 * Allocated/Alive/Dead tuples:
 *   Tuples in data are called allocated tuples.
 *   Tuples in free_tuples are called dead tuples.
 *   Tuples in data but not in free_tuples are called alive tuples.
 *
 * Tile objects including derived classes are only instantiated via TileFactory.
 */

class TileIterator;

class Tile {
	friend class TileIterator;
	//friend class TileStats;

	Tile() = delete;
	Tile(Tile const&) = delete;

public:
	virtual ~Tile();

	//===--------------------------------------------------------------------===//
	// Operations and stats
	//===--------------------------------------------------------------------===//

	//virtual bool InsertTuple(Tuple &source) = 0;
	//virtual bool UpdateTuple(Tuple &source, Tuple &target, bool update_indexes) = 0;
	//virtual bool DeleteTuple(Tuple &tuple, bool free_uninlined_columns) = 0;

	//virtual void DeleteAllTuples(bool freeAllocatedStrings) = 0;

	Tuple& TempTuple() {
		assert(!temp_tuple.IsNull());
		return temp_tuple;
	}

	// allocated tuples in tile
	int64_t GetAllocatedTupleCount() const {
		return allocated_tuple_count;
	}

	// active tuples in tile
	virtual int64_t GetActiveTupleCount() const {
		return active_tuple_count;
	}

	int64_t GetOccupiedSize() const {
		return active_tuple_count * temp_tuple.Length();
	}

	int64_t GetUninlinedDataSize() const {
		return uninlined_data_size;
	}

	/// Tile lifespan can be managed by a reference count.
	void IncrementRefcount() {
		tile_ref_count += 1;
	}

	void DecrementRefcount() {
		tile_ref_count -= 1;
		if (tile_ref_count == 0) {
			delete this;
		}
	}

	/// Estimate the number of times that tuples are accessed (either for a read or write)
	inline int64_t GetAccessCount() const {
		return tile_ref_count;
	}

	inline void IncrementAccessCount() {
		tile_ref_count++;
	}

	int GetTupleIndex(const char *tuple_address) const;

	int GetColumnIndex(const std::string &name) const;

	/// Only inlined data
	uint32_t GetSize() const {
		return tile_size ;
	}

	/// Both inlined and uninlined data
	uint32_t GetTotalSize() const {
		return tile_size + uninlined_data_size ;
	}

	/// Compare two tiles
	virtual bool operator== (const Tile &other) const;
	virtual bool operator!= (const Tile &other) const;

	virtual TileIterator GetTileIterator();

	//virtual TileStats* GetTileStats();

	//===--------------------------------------------------------------------===//
	// Columns
	//===--------------------------------------------------------------------===//

	inline const catalog::Schema* GetSchema() const {
		return schema;
	};

	inline const std::string& GetColumnName(int index) const {
		return column_names[index];
	}

	inline int GetColumnCount() const {
		return column_count;
	};

	const std::vector<std::string> GetColumns() const {
		return column_names;
	}

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	//virtual std::string TileType() const = 0;

	/// Get a string representation of this tile
	friend std::ostream& operator<<(std::ostream& os, const Tile& tile);

	//===--------------------------------------------------------------------===//
	// Serialization/Deserialization
	//===--------------------------------------------------------------------===//

	bool SerializeTo(SerializeOutput &output);

	bool SerializeHeaderTo(SerializeOutput &output);

	/// Serialize a subset of tuples as a tile
	bool SerializeTuplesTo(SerializeOutput &output, Tuple *tuples, int num_tuples);

	/**
	 * Loads only tuple data, not schema, from the serialized tile.
	 * Used for initial data loading.
	 * @param allow_export if false, export enabled is overriden for this load.
	 */
	void DeserializeTuplesFrom(SerializeInput &serialize_in, Pool *pool = NULL);

	/**
	 * Loads only tuple data and assumes there is no schema present.
	 * Used for recovery where the schema is not sent.
	 * @param allow_export if false, export enabled is overriden for this load.
	 */
	void DeserializeTuplesFromWithoutHeader(SerializeInput &input, Pool *pool = NULL);

	Pool* GetPool(){
		return (pool);
	}

	/// Tile creator
	Tile(catalog::Schema *tuple_schema, int tuple_count,
			const std::vector<std::string>& column_names, bool own_schema);

protected:

	/// Reset tile
	void Reset();

	/// Move tuple to next free slot.
	/// Returns false if no such slot exists in Tile.
	bool NextFreeTuple(Tuple *tuple);

	char *GetTupleLocation(const int index) const;

	/// return the tuple storage to the free list.
	inline void DeleteTupleStorage(Tuple &tuple);

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	/// set of fixed-length tuple slots
	char *data;

	/// storage pool for uninlined data
	Pool *pool;

	/// column header
	std::vector<std::string> column_names;

	/// reusable temp tuple
	Tuple temp_tuple;

	/// not temp tuples. These are for internal use.
	Tuple temp_target1, temp_target2;

	/// tile schema
	catalog::Schema *schema;

	/// do we need to free this in destructor ?
	bool own_schema;

	/// allocated tuples
	int allocated_tuple_count;

	/// active tuples
	int active_tuple_count;

	/// next free slot iterator
	int next_tuple_itr;

	/// tile reference count
	int tile_ref_count;

	/// number of columns in tile
	int column_count;

	/// length of tuple in tile
	int tuple_length;

	/// size of the tile (only inlined data)
	int tile_size;

	/// memory occupied by uninlined data
	int64_t uninlined_data_size;

	/**
	 * List of pointers to <b>once used and then deleted</b> tuples.
	 * Tuples after used_tuples index are also free, this queue
	 * is used to find "hole" tuples which were once used
	 * (before used_tuples index) and also deleted.
	 * NOTE THAT THESE ARE NOT THE ONLY FREE TUPLES.
	 */
	std::vector<char*> free_tuple_slots;

	/// Catalog information
	int tile_group_id;

	int table_id;

	int database_id;

	/// Used for serialization/deserialization
	char *column_header;

	int32_t column_header_size;
};

/// Returns a pointer to the tuple requested. No checks are done that the index is valid.
inline char* Tile::GetTupleLocation(const int index) const {
	char *tuple_location = data + ((index % allocated_tuple_count) * tuple_length);

	return tuple_location;
}

/// Finds index of tuple for a given tuple address.
/// Returns -1 if no matching tuple was found
inline int Tile::GetTupleIndex(const char* tuple_address) const{

	/// check if address within tile bounds
	if((tuple_address < data) || (tuple_address >= (data + tile_size)))
		return -1;

	int tuple_id = 0;

	/// check if address is at an offset that is an integral multiple of tuple length
	tuple_id = (tuple_address - data)/tuple_length;

	if(tuple_id * tuple_length + data == tuple_address)
		return tuple_id;

	return -1;
}

inline void Tile::DeleteTupleStorage(Tuple &tuple) {
	tuple.SetDeletedTrue(); // does NOT free uninlined data

	tuple.FreeColumns();

	// add tuple slot to the free list
	active_tuple_count--;

	free_tuple_slots.push_back(tuple.Location());
}


} // End storage namespace
} // End nstore namespace

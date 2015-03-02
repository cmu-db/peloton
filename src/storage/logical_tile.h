/*-------------------------------------------------------------------------
*
* logical_tile.h
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/logical_tile.h
*
*-------------------------------------------------------------------------
*/


#pragma once

#include "catalog/schema.h"
#include "storage/tuple.h"

#include "storage/physical_tile.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Logical Tile
//===--------------------------------------------------------------------===//

class TileIterator;

/**
 * Represents a logical tile.
 *
 * Has one and exactly one base PhysicalTile. Useful for cheaply
 * projecting and/or selecting a subset of a physical tile.
 *
 */
class LogicalTile : public Tile {
	friend class TileIterator;
	friend class TileFactory;
	//friend class TileStats;

	LogicalTile() = delete;
	LogicalTile(LogicalTile const&) = delete;

public:

	/// Tile creator
	LogicalTile(catalog::Schema *tuple_schema, int tuple_count,
			const std::vector<std::string>& column_names, bool own_schema);

	~LogicalTile();

	//===--------------------------------------------------------------------===//
	// Operations and stats
	//===--------------------------------------------------------------------===//

	bool InsertTuple(Tuple &source);
	bool UpdateTuple(Tuple &source, Tuple &target, bool update_indexes);
	bool DeleteTuple(Tuple &tuple, bool free_uninlined_columns);
	void DeleteAllTuples(bool freeAllocatedStrings);

	Tuple& TempTuple();
	int64_t GetAllocatedTupleCount() const;
	int64_t GetActiveTupleCount() const;
	int GetTupleOffset(const char *tuple_address) const;
	int GetColumnOffset(const std::string &name) const;

	//===--------------------------------------------------------------------===//
	// Size Stats
	//===--------------------------------------------------------------------===//

	uint32_t GetInlinedSize() const;
	int64_t GetUninlinedDataSize() const;
	uint32_t GetSize() const;
	int64_t GetOccupiedSize() const;

	//===--------------------------------------------------------------------===//
	// Columns
	//===--------------------------------------------------------------------===//

	const catalog::Schema* GetSchema();
	const std::string& GetColumnName(int index);
	int GetColumnCount() const;
	const std::vector<std::string> GetColumns();

	//===--------------------------------------------------------------------===//
	// Ref counting
	//===--------------------------------------------------------------------===//

	/// Tile lifespan can be managed by a reference count.
	void IncrementRefcount();
	void DecrementRefcount();

	/// Estimate the number of times that tuples are accessed (either for a read or write)
	int64_t GetAccessCount();
	void IncrementAccessCount();

	/// Compare two tiles
	bool operator== (const LogicalTile &other) const;
	bool operator!= (const LogicalTile &other) const;

	TileIterator GetIterator() {
		return base_tile->GetIterator();
	}

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	std::string GetTileType() const{
		return GetTileTypeName(TILE_TYPE_LOGICAL);
	}

	/// Get a string representation of this tile
	friend std::ostream& operator<<(std::ostream& os, const LogicalTile& tile);

	//===--------------------------------------------------------------------===//
	// Serialization/Deserialization
	//===--------------------------------------------------------------------===//

	bool SerializeTo(SerializeOutput &output) {
		return base_tile->SerializeTo(output);
	}

	bool SerializeHeaderTo(SerializeOutput &output){
		return base_tile->SerializeHeaderTo(output);
	}

	bool SerializeTuplesTo(SerializeOutput &output, Tuple *tuples, int num_tuples){
		return base_tile->SerializeTuplesTo(output, tuples, num_tuples);
	}

	void DeserializeTuplesFrom(SerializeInput &input, Pool *pool = NULL) {
		base_tile->DeserializeTuplesFrom(input, pool);
	}

	void DeserializeTuplesFromWithoutHeader(SerializeInput &input, Pool *pool = NULL){
		base_tile->DeserializeTuplesFromWithoutHeader(input, pool);
	}

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	PhysicalTile *base_tile;

	/// Status of columns
	std::bitset columns;

	/// Status of rows
	std::bitset rows;
};


} // End storage namespace
} // End nstore namespace




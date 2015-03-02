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

//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

class TileIterator;

/**
 * Represents a tile. Look at @PhysicalTile and @LogicalTile for more details.
 *
 * Tiles including derived classes are only instantiated via TileFactory.
 */
class Tile {
	friend class TileIterator;
	//friend class TileStats;

public:
	virtual ~Tile(){};

	//===--------------------------------------------------------------------===//
	// Operations and stats
	//===--------------------------------------------------------------------===//

	virtual bool InsertTuple(Tuple &source) = 0;
	virtual bool UpdateTuple(Tuple &source, Tuple &target, bool update_indexes) = 0;
	virtual bool DeleteTuple(Tuple &tuple, bool free_uninlined_columns) = 0;
	virtual void DeleteAllTuples(bool freeAllocatedStrings) = 0;

	virtual Tuple& TempTuple() = 0;

	virtual int64_t GetAllocatedTupleCount() const = 0;
	virtual int64_t GetActiveTupleCount() const = 0;
	virtual int64_t GetOccupiedSize() const = 0;
	virtual int64_t GetUninlinedDataSize() const = 0;

	/// Tile lifespan can be managed by a reference count.
	virtual void IncrementRefcount() = 0;
	virtual void DecrementRefcount() = 0;

	/// Estimate the number of times that tuples are accessed (either for a read or write)
	virtual int64_t GetAccessCount() const = 0;
	virtual void IncrementAccessCount() = 0;

	virtual uint32_t GetInlinedSize() const = 0;
	virtual uint32_t GetSize() const = 0;

	virtual int GetTupleOffset(const char *tuple_address) const = 0;
	virtual int GetColumnOffset(const std::string &name) const = 0;

	virtual TileIterator GetIterator() = 0;
	//virtual TileStats* GetStats() = 0;

	//===--------------------------------------------------------------------===//
	// Columns
	//===--------------------------------------------------------------------===//

	virtual const catalog::Schema* GetSchema() const = 0;
	virtual const std::string& GetColumnName(int index) const  = 0;
	virtual int GetColumnCount() const = 0;
	virtual const std::vector<std::string> GetColumns() const = 0;

	//===--------------------------------------------------------------------===//
	// Utilities
	//===--------------------------------------------------------------------===//

	virtual std::string GetTileType() const = 0;

	/// Get a string representation of this tile
	friend std::ostream& operator<<(std::ostream& os, const Tile& tile);

	//===--------------------------------------------------------------------===//
	// Serialization/Deserialization
	//===--------------------------------------------------------------------===//

	virtual bool SerializeTo(SerializeOutput &output) = 0;
	virtual bool SerializeHeaderTo(SerializeOutput &output) = 0;
	virtual bool SerializeTuplesTo(SerializeOutput &output, Tuple *tuples, int num_tuples) = 0;

	virtual void DeserializeTuplesFrom(SerializeInput &serialize_in, Pool *pool = NULL) = 0;
	virtual void DeserializeTuplesFromWithoutHeader(SerializeInput &input, Pool *pool = NULL) = 0;

};

} // End storage namespace
} // End nstore namespace

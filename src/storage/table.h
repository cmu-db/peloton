/*-------------------------------------------------------------------------
*
* table.h
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/table.h
*
*-------------------------------------------------------------------------
*/

#pragma once

#include <string>

#include "storage/tuple.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Table
//===--------------------------------------------------------------------===//

/**
 * Represents a group of tile groups logically vertically contiguous.
 *
 * <Tile Group 1>
 * <Tile Group 2>
 * ...
 * <Tile Group n>
 *
 */
class Table {
public:
	Table();
	virtual ~Table();

	int GetColumnCount();

	void *Get();

	std::string GetColumnName(int column_itr);

	bool InsertTuple(Tuple tuple);

	Tuple TempTuple();

	void Reset(Table t);

protected:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	/// set of tile groups
	std::vector<TileGroup*> tiles;

	/// Catalog information
	Oid table_id;
	Oid database_id;

};

} // End storage namespace
} // End nstore namespace



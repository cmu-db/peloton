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

/*
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
};

} // End storage namespace
} // End nstore namespace



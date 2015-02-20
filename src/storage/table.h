/*-------------------------------------------------------------------------
*
* abstract_table.h
* the base class for all tables
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/abstract_table.h
*
*-------------------------------------------------------------------------
*/

#pragma once

namespace nstore {
namespace storage {

#include "tuple.h"

class Table	{
	class AbstractTuple;
	class TuplePointer;

public:
	virtual ~Table();

	virtual bool insertTuple(AbstractTuple &source) = 0;
	virtual bool updateTuple(AbstractTuple &source, AbstractTuple &target, bool updatesIndexes) = 0;
	virtual bool deleteTuple(AbstractTuple &tuple, bool deleteAllocatedStrings) = 0;


	virtual AbstractTuple getTuple(const TuplePointer) = 0;
};


} // End storage namespace
} // End nstore namespace

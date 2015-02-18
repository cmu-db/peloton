/*-------------------------------------------------------------------------
 *
 * abstract_tuple.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/abstract_tuple.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/abstract_tuple_schema.h"
#include "common/types.h"
#include "common/value.h"

namespace nstore {
namespace storage {

class AbstractTuple	{
	friend class TupleSchema;
	friend class catalog::AbstractTupleSchema;

public:
	virtual ~AbstractTuple();

	void SetField(const int column_id, Value value);

	Value GetField(const int column_id);

	catalog::AbstractTupleSchema *GetTupleSchema();

protected:
	char *tuple_data;
	const catalog::AbstractTupleSchema *tuple_schema;

private:

	inline char *GetFieldPtr(const uint32_t column_id) {
		return &tuple_data[tuple_schema->GetColumnOffset(column_id) +
						   tuple_schema->GetTupleHeaderSize()];
	}

};

class TuplePointer{

public:
	uint16_t	tile_id;
	uint16_t	tile_offset;

};

} // End storage namespace
} // End nstore namespace


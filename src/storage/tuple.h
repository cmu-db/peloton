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

#include "catalog/tuple_schema.h"
#include "common/value.h"
#include "common/types.h"

namespace nstore {
namespace storage {

class Tuple	{
	friend class TupleSchema;
	friend class catalog::TupleSchema;

public:
	virtual ~Tuple();

	void SetField(const int column_id, Value value);

	Value GetField(const int column_id);

	catalog::TupleSchema *GetTupleSchema();

protected:
	char *tuple_data;
	const catalog::TupleSchema *tuple_schema;

private:

	inline char *GetFieldPtr(const uint32_t column_id) {
		return &tuple_data[tuple_schema->Offset(column_id)];
	}

};

class TuplePointer{

public:
	uint16_t	tile_id;
	uint16_t	tile_offset;

};

} // End storage namespace
} // End nstore namespace


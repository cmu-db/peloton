/*
 * tupleTransformer.h
 *
 *  Created on: Jun 22, 2015
 *      Author: vivek
 */

#pragma once

extern "C" {
#include "postgres.h"
#include "executor/tuptable.h"
}

#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/storage/data_table.h"


//typedef nstore::Value nstore_value;

extern "C" {

	nstore::Value DatumGetValue(Datum datum, Oid atttypid);

	Datum ValueGetDatum(nstore::Value value);

	void TestTupleTransformer(Datum datum, Oid atttypid);

	nstore::storage::Tuple *TupleTransformer(TupleTableSlot *slot);

};

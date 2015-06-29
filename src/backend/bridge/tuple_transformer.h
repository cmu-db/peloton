/*
 * tupleTransformer.h
 *
 *  Created on: Jun 22, 2015
 *      Author: vivek
 */

#pragma once

#include "postgres.h"
#include "executor/tuptable.h"

#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/storage/data_table.h"

//typedef peloton::Value peloton_value;


peloton::Value DatumGetValue(Datum datum, Oid atttypid);

Datum ValueGetDatum(peloton::Value value);

void TestTupleTransformer(Datum datum, Oid atttypid);


namespace peloton {
namespace bridge {
storage::Tuple *TupleTransformer(TupleTableSlot *slot, const catalog::Schema *schema);
}
}

/**
 * @brief Header for postgres plan transformer.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

extern "C" {
#include "postgres.h"
#include "executor/tuptable.h"
}

#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/storage/data_table.h"

//typedef peloton::Value peloton_value;

extern "C" {

peloton::Value DatumGetValue(Datum datum, Oid atttypid);

Datum ValueGetDatum(peloton::Value value);

void TestTupleTransformer(Datum datum, Oid atttypid);

}

namespace peloton {
namespace bridge {
storage::Tuple *TupleTransformer(TupleTableSlot *slot, const catalog::Schema *schema);
}
}

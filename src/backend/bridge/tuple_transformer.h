/**
 * @brief Header for tuple transformer.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "postgres.h"
#include "executor/tuptable.h"

#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Tuple Transformer
//===--------------------------------------------------------------------===//

Value DatumGetValue(Datum datum, Oid atttypid);

Datum ValueGetDatum(peloton::Value value);

storage::Tuple *TupleTransformer(TupleTableSlot *slot, const catalog::Schema *schema);


}
}
